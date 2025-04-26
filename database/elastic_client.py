import logging
import os
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

from config.api_config import get_api_config
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ElasticClient:
    """
    Client for Elasticsearch operations.
    Handles entity resolution and semantic search for ontology terms.
    """
    
    def __init__(self, url: Optional[str] = None):
        """
        Initialize the Elasticsearch client.
        
        Args:
            url: URL of the Elasticsearch server, defaults to config or localhost.
        """
        # Get config
        elasticsearch_config = get_api_config("elasticsearch")
        self.url = url or elasticsearch_config["url"]

        # Initialize the client
        self.client = Elasticsearch(hosts=[self.url])
        
        # Define index mappings for different entity types
        self.entity_indices = elasticsearch_config["indices"]
        
        # Default number of results to return
        self.default_size = elasticsearch_config["search_limit"]

        # Create query-sparql index if it doesn't exist
        self._create_query_sparql_index()
    
    def _create_query_sparql_index(self):
        """
        Create the query-sparql index if it doesn't exist.
        """
        index_name = "nl2sparql-queries"
        
        # Check if index already exists
        if not self.client.indices.exists(index=index_name):
            mappings = {
                "properties": {
                    "natural_query": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "context": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "sparql_query": {
                        "type": "text",
                        "index": True
                    },
                    "response": {
                        "type": "text",
                        "index": True
                    },
                    "timestamp": {
                        "type": "date"
                    },
                    "execution_time": {
                        "type": "float"
                    },
                    "successful": {
                        "type": "boolean"
                    }
                }
            }
            
            try:
                self.create_index(index_name, mappings)
                logger.info(f"Created nl2sparql-queries index in Elasticsearch")
            except Exception as e:
                logger.error(f"Error creating nl2sparql-queries index: {e}")
    
    def create_index(self, index_name: str, mappings: Dict[str, Any]) -> bool:
        """
        Create a new index with specified mappings.
        
        Args:
            index_name: Name of the index
            mappings: Index mappings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create the index with settings and mappings.
            response = self.client.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "ontology_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            }
                        }
                    },
                    "mappings": mappings
                }
            )
            return response.get("acknowledged", False)
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            return False
    
    def index_ontology_class(self, class_data: Dict[str, Any]) -> bool:
        """
        Index an ontology class.
        
        Args:
            class_data: Class data to index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure document has a URI
            uri = class_data.get("uri")
            if not uri:
                logger.error("Error: Class document must have a 'uri' field")
                return False
            
            # Prepare the document
            doc = {
                "uri": uri,
                "label": class_data.get("label", ""),
                "comment": class_data.get("comment", ""),
                "superclasses": class_data.get("superclasses", []),
                "subclasses": class_data.get("subclasses", []),
                "properties": class_data.get("properties", [])
            }
            
            # Add aliases (alternative labels)
            if "aliases" in class_data:
                doc["aliases"] = class_data["aliases"]
            
            # Index the document
            response = self.client.index(
                index=self.entity_indices["TOKEN"],
                id=uri,
                document=doc,
                refresh=True  # Ensure document is immediately searchable
            )
            return response.get("result") in ["created", "updated"]
        except Exception as e:
            logger.error(f"Error indexing ontology class: {e}")
            return False
    
    def index_ontology_property(self, property_data: Dict[str, Any]) -> bool:
        """
        Index an ontology property.
        
        Args:
            property_data: Property data to index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure document has a URI
            uri = property_data.get("uri")
            if not uri:
                logger.error("Error: Property document must have a 'uri' field")
                return False
            
            # Prepare the document
            doc = {
                "uri": uri,
                "label": property_data.get("label", ""),
                "comment": property_data.get("comment", ""),
                "type": property_data.get("type", "rdf:Property"),
                "domains": property_data.get("domains", []),
                "ranges": property_data.get("ranges", [])
            }
            
            # Add aliases (alternative labels)
            if "aliases" in property_data:
                doc["aliases"] = property_data["aliases"]
            
            # Index the document
            response = self.client.index(
                index=self.entity_indices["PROPERTY"],
                id=uri,
                document=doc,
                refresh=True  # Ensure document is immediately searchable
            )
            return response.get("result") in ["created", "updated"]
        except Exception as e:
            logger.error(f"Error indexing ontology property: {e}")
            return False
    
    def index_ontology_instance(self, instance_data: Dict[str, Any]) -> bool:
        """
        Index an ontology instance.
        
        Args:
            instance_data: Instance data to index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure document has a URI
            uri = instance_data.get("uri")
            if not uri:
                logger.error("Error: Instance document must have a 'uri' field")
                return False
            
            # Prepare the document
            doc = {
                "uri": uri,
                "label": instance_data.get("label", ""),
                "types": instance_data.get("types", []),
                "properties": instance_data.get("properties", {})
            }
            
            # Index the document
            response = self.client.index(
                index=self.entity_indices["INSTANCE"],
                id=uri,
                document=doc,
                refresh=True  # Ensure document is immediately searchable
            )
            return response.get("result") in ["created", "updated"]
        except Exception as e:
            logger.error(f"Error indexing ontology instance: {e}")
            return False
    
    def bulk_index_ontology(
        self, 
        classes: List[Dict[str, Any]], 
        properties: List[Dict[str, Any]], 
        instances: List[Dict[str, Any]]
    ) -> bool:
        """
        Bulk index ontology elements.
        
        Args:
            classes: List of class data to index
            properties: List of property data to index
            instances: List of instance data to index
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare bulk indexing operations
            operations = []
            
            # Add classes
            for cls in classes:
                uri = cls.get("uri")
                if not uri:
                    continue
                
                operations.append({
                    "index": {
                        "_index": self.entity_indices["TOKEN"],
                        "_id": uri
                    }
                })
                operations.append(cls)
            
            # Add properties
            for prop in properties:
                uri = prop.get("uri")
                if not uri:
                    continue
                
                operations.append({
                    "index": {
                        "_index": self.entity_indices["PROPERTY"],
                        "_id": uri
                    }
                })
                operations.append(prop)
            
            # Add instances
            for inst in instances:
                uri = inst.get("uri")
                if not uri:
                    continue
                
                operations.append({
                    "index": {
                        "_index": self.entity_indices["INSTANCE"],
                        "_id": uri
                    }
                })
                operations.append(inst)
            
            # Execute bulk operation
            if not operations:
                logger.warning("No valid entities to index")
                return False
            
            response = self.client.bulk(operations=operations, refresh=True)
            success = not response.get("errors", True)
            
            if success:
                logger.info(f"Bulk indexed {len(classes)} classes, {len(properties)} properties, and {len(instances)} instances")
            else:
                logger.error(f"Errors in bulk indexing: {response.get('items', [])}")
            
            return success
        except Exception as e:
            logger.error(f"Error bulk indexing ontology: {e}")
            return False
    
    def search_ontology_term(
        self, 
        query: str, 
        entity_type: str = None, 
        size: int = None
    ) -> List[Dict[str, Any]]:
        """
        Search for ontology terms matching a query.
        
        Args:
            query: Text to search for
            entity_type: Type of entity to search for (TOKEN, PROPERTY, INSTANCE)
            size: Maximum number of results
            
        Returns:
            List of matching terms
        """
        # Use default size if not specified
        if size is None:
            size = self.default_size
        
        # Determine which indices to search
        if entity_type and entity_type in self.entity_indices:
            indices = [self.entity_indices[entity_type]]
        else:
            # Search all indices if no specific type given
            indices = list(self.entity_indices.values())
        
        try:
            # Create multi-field search query with various matching techniques
            response = self.client.search(
                index=indices,
                body={
                    "size": size,
                    "query": {
                        "bool": {
                            "should": [
                                # Exact match on label field (highest priority)
                                {
                                    "match": {
                                        "label": {
                                            "query": query,
                                            "boost": 3
                                        }
                                    }
                                },
                                # Fuzzy match on label field
                                {
                                    "match": {
                                        "label": {
                                            "query": query,
                                            "fuzziness": "AUTO",
                                            "boost": 2
                                        }
                                    }
                                },
                                # Search in aliases
                                {
                                    "match": {
                                        "aliases": {
                                            "query": query,
                                            "boost": 1.5
                                        }
                                    }
                                },
                                # Search in comment
                                {
                                    "match": {
                                        "comment": {
                                            "query": query,
                                            "boost": 1
                                        }
                                    }
                                },
                                # Wildcard search (prefix/suffix matching)
                                {
                                    "wildcard": {
                                        "label": {
                                            "value": f"*{query}*",
                                            "boost": 1
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            )
            
            # Extract and format the results
            hits = response.get("hits", {}).get("hits", [])
            results = []
            
            for hit in hits:
                source = hit.get("_source", {})
                # Add metadata about the hit
                source["_score"] = hit.get("_score", 0)
                source["_index"] = hit.get("_index", "")
                
                # Determine entity type from index
                for type_key, index_name in self.entity_indices.items():
                    if index_name == source["_index"]:
                        source["entity_type"] = type_key
                        break
                
                results.append(source)
            
            return results
        except Exception as e:
            logger.error(f"Error searching for ontology term: {e}")
            return []
    
    def initialize_indices(self) -> bool:
        """
        Initialize all required indices with appropriate mappings.

        Returns:
            True if all indices were created or already exist
        """
        try:
            # Class/Token index mapping
            class_mapping = {
                "properties": {
                    "uri": {"type": "keyword"},
                    "label": {
                        "type": "text",
                        "analyzer": "ontology_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "comment": {"type": "text", "analyzer": "ontology_analyzer"},
                    "aliases": {"type": "text", "analyzer": "ontology_analyzer"},
                    "superclasses": {"type": "keyword"},
                    "subclasses": {"type": "keyword"},
                    "properties": {"type": "keyword"}
                }
            }
            
            # Property index mapping
            property_mapping = {
                "properties": {
                    "uri": {"type": "keyword"},
                    "label": {
                        "type": "text",
                        "analyzer": "ontology_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "comment": {"type": "text", "analyzer": "ontology_analyzer"},
                    "aliases": {"type": "text", "analyzer": "ontology_analyzer"},
                    "type": {"type": "keyword"},
                    "domains": {"type": "keyword"},
                    "ranges": {"type": "keyword"}
                }
            }
            
            # Instance index mapping
            instance_mapping = {
                "properties": {
                    "uri": {"type": "keyword"},
                    "label": {
                        "type": "text",
                        "analyzer": "ontology_analyzer",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "types": {"type": "keyword"},
                    "properties": {
                        "type": "object",
                        "enabled": True
                    }
                }
            }
            
            # Literal index mapping
            literal_mapping = {
                "properties": {
                    "text": {"type": "text", "analyzer": "ontology_analyzer"},
                    "datatype": {"type": "keyword"},
                    "language": {"type": "keyword"}
                }
            }
            
            # Mapping of index names to their mappings
            index_mappings = {
                self.entity_indices["TOKEN"]: class_mapping,
                self.entity_indices["PROPERTY"]: property_mapping,
                self.entity_indices["INSTANCE"]: instance_mapping,
                self.entity_indices["LITERAL"]: literal_mapping
            }
            
            # Create each index if it doesn't exist.
            success = True
            for index_name, mappings in index_mappings.items():
                if not self.index_exists(index_name):
                    logger.info(f"Creating index: {index_name}")
                    success = success and self.create_index(index_name, mappings)
                    
            return success
        except Exception as e:
            logger.error(f"Error initializing indices: {e}")
            return False
    
    def index_exists(self, index_name: str) -> bool:
        """
        Check if an index exists.
        
        Args:
            index_name: Name of the index
            
        Returns:
            True if exists, False otherwise
        """
        try:
            return self.client.indices.exists(index=index_name)
        except Exception as e:
            logger.error(f"Error checking index: {e}")
            return False

    def store_query_sparql_pair(self, query_data: Dict[str, Any]) -> bool:
        """
        Store a natural language query and its corresponding SPARQL query in Elasticsearch.
        
        Args:
            query_data: Dictionary containing:
                - natural_query: The natural language query
                - sparql_query: The corresponding SPARQL query
                - context: Optional context information
                - response: Generated response
                - successful: Whether query execution was successful
                - execution_time: Time taken to process the query
                
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure required fields are present
            if "natural_query" not in query_data or "sparql_query" not in query_data:
                logger.error("Missing required fields in query data")
                return False
            
            # Prepare document
            doc = {
                "natural_query": query_data["natural_query"],
                "sparql_query": query_data["sparql_query"],
                "context": query_data.get("context", ""),
                "response": query_data.get("response", ""),
                "timestamp": query_data.get("timestamp", time.time()),
                "execution_time": query_data.get("execution_time", 0),
                "successful": query_data.get("successful", True)
            }
            
            # Index the document
            response = self.client.index(
                index="nl2sparql-queries",
                document=doc,
                refresh=True  # Ensure document is immediately searchable
            )
            
            success = response.get("result") in ["created", "updated"]
            if success:
                logger.info(f"Stored query-SPARQL pair for: {query_data['natural_query'][:50]}...")
            
            return success
            
        except Exception as e:
            logger.error(f"Error storing query-SPARQL pair: {e}")
            return False
    
    def search_similar_query(self, natural_query: str, min_score: float = 0.8) -> Optional[Dict[str, Any]]:
        """
        Search for semantically similar queries in Elasticsearch.
        
        Args:
            natural_query: The natural language query to search for
            min_score: Minimum relevance score (0.0 to 1.0)
            
        Returns:
            Dictionary with match results or None if no good match found
        """
        try:
            # Prepare query
            search_query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "natural_query": {
                                        "query": natural_query,
                                        "fuzziness": "AUTO"
                                    }
                                }
                            },
                            {
                                "term": {
                                    "successful": True
                                }
                            }
                        ]
                    }
                },
                "size": 1  # Only need the best match
            }
            
            # Execute search
            response = self.client.search(
                index="nl2sparql-queries",
                body=search_query
            )
            
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                logger.debug(f"No similar query found for: {natural_query[:50]}...")
                return None
                
            best_match = hits[0]
            score = best_match.get("_score", 0)
            max_score = response.get("hits", {}).get("max_score", 1)
            normalized_score = score / max_score if max_score > 0 else 0
            
            if normalized_score < min_score:
                logger.debug(f"Similar query found but score too low ({normalized_score:.2f}): {natural_query[:50]}...")
                return None
                
            result = best_match.get("_source", {})
            result["score"] = normalized_score
            
            logger.info(f"Found similar query (score: {normalized_score:.2f}): {result.get('natural_query', '')[:50]}...")
            return result
            
        except Exception as e:
            logger.error(f"Error searching for similar query: {e}")
            return None
