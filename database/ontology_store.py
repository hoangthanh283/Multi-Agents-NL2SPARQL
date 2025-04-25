import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import redis
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD
from SPARQLWrapper import JSON, SPARQLWrapper

logger = logging.getLogger(__name__)

def cache_result(expire_time=3600):
    """Redis cache decorator"""
    def decorator(f):
        @wraps(f)
        def decorated_function(self, *args, **kwargs):
            if not hasattr(self, 'redis_client'):
                return f(self, *args, **kwargs)
                
            cache_key = f"{f.__name__}:{str(args)}:{str(kwargs)}"
            result = self.redis_client.get(cache_key)
            
            if result is not None:
                return pd.read_json(result)
                
            result = f(self, *args, **kwargs)
            if isinstance(result, pd.DataFrame):
                self.redis_client.setex(cache_key, expire_time, result.to_json())
            return result
        return decorated_function
    return decorator

class OntologyStore:
    """
    Store for managing access to ontology data.
    Handles loading, querying, and caching of ontology information.
    """
    
    def __init__(
        self, 
        local_path: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        prefixes: Optional[Dict[str, str]] = None,
        redis_url: Optional[str] = "redis://localhost:6379/0",
        max_workers: int = 10
    ):
        """Initialize the ontology store with Redis cache and thread pool."""
        self.local_path = local_path
        self.endpoint_url = endpoint_url if endpoint_url else "http://localhost:7200/repositories/CHeVIE"
        
        # Initialize Redis client
        try:
            self.redis_client = redis.from_url(redis_url)
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self.redis_client = None
            
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Initialize SPARQL wrapper with connection pooling
        self.sparql = SPARQLWrapper(self.endpoint_url)
        self.sparql.setReturnFormat(JSON)
        
        # Initialize RDF graph for local file
        self.graph = Graph()
        
        # Initialize prefixes
        self.prefixes = prefixes or {
            "rdf": str(RDF),
            "rdfs": str(RDFS),
            "owl": str(OWL),
            "xsd": str(XSD)
        }
        
        # Add prefixes to the graph
        for prefix, uri in self.prefixes.items():
            self.graph.bind(prefix, uri)
        
        # Cache for ontology structure
        self.classes = {}
        self.properties = {}
        self.instances = {}
        
        # Statistics
        self.stats = {
            "total_triples": 0,
            "class_count": 0,
            "property_count": 0,
            "instance_count": 0
        }
    
    def load_ontology(self) -> bool:
        """
        Load ontology data from local file or endpoint.
        
        Returns:
            True if successful, False otherwise
        """
        success = False
        
        # Try loading from endpoint first
        try:
            # Test connection to GraphDB
            logger.info(f"Testing connection to GraphDB: {self.endpoint_url}")
            
            # Simple query to check connection
            test_query = "ASK { ?s ?p ?o }"
            self.sparql.setQuery(test_query)
            results = self.sparql.query().convert()
            
            if results.get('boolean', False):
                logger.info("Successfully connected to GraphDB")
                success = True
                
                # Load basic statistics
                self._load_basic_stats()
                
                # We don't need to load the full ontology into memory
                # We'll query GraphDB directly as needed
                return True
            else:
                logger.warning("Connected to GraphDB but repository might be empty")
        except Exception as e:
            logger.error(f"Error connecting to GraphDB: {e}")
        
        # If GraphDB loading failed, try loading from local file
        if not success and self.local_path and os.path.exists(self.local_path):
            success = self._load_from_file()
        
        # If either loading method was successful, build indices
        if success:
            self._update_stats()
            logger.info(f"Loaded ontology with {self.stats['total_triples']} triples")
            return True
        
        logger.warning("Failed to load ontology from any source")
        return False
    
    def _load_from_file(self) -> bool:
        """
        Load ontology from a local file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading ontology from file: {self.local_path}")
            self.graph.parse(self.local_path)
            return len(self.graph) > 0
        except Exception as e:
            logger.error(f"Error loading ontology from file: {e}")
            return False
    
    def _load_basic_stats(self):
        """Load basic statistics about the ontology from GraphDB."""
        try:
            # Get total triple count
            query = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
            results_df = self._query_graphdb(query)
            
            if not results_df.empty:
                self.stats["total_triples"] = int(results_df["count"].iloc[0])
            
            # Get class count
            query = """
            SELECT (COUNT(DISTINCT ?class) AS ?count) 
            WHERE { 
                { ?class a owl:Class } 
                UNION 
                { ?class a rdfs:Class }
            }
            """
            results_df = self._query_graphdb(query)
            
            if not results_df.empty:
                self.stats["class_count"] = int(results_df["count"].iloc[0])
            
            # Get property count
            query = """
            SELECT (COUNT(DISTINCT ?prop) AS ?count) 
            WHERE { 
                { ?prop a rdf:Property } 
                UNION 
                { ?prop a owl:ObjectProperty }
                UNION 
                { ?prop a owl:DatatypeProperty }
            }
            """
            results_df = self._query_graphdb(query)
            
            if not results_df.empty:
                self.stats["property_count"] = int(results_df["count"].iloc[0])
            
            # Estimate instance count
            query = """
            SELECT (COUNT(DISTINCT ?instance) AS ?count) 
            WHERE { 
                ?instance a ?type .
                ?type a owl:Class .
                FILTER(?type != owl:Class && ?type != rdfs:Class)
            }
            LIMIT 1
            """
            results_df = self._query_graphdb(query)
            
            if not results_df.empty:
                self.stats["instance_count"] = int(results_df["count"].iloc[0])
                
            logger.info(f"Loaded ontology stats: {self.stats}")
            
        except Exception as e:
            logger.error(f"Error loading basic stats: {e}")
    
    def _build_indices(self):
        """Build indices for faster access to ontology elements."""
        logger.info("Building ontology indices...")
        
        # Index classes
        for class_uri in self.graph.subjects(RDF.type, OWL.Class):
            if isinstance(class_uri, URIRef):
                class_info = self._extract_class_info(class_uri)
                self.classes[str(class_uri)] = class_info
        
        # Index properties
        for prop_uri in self.graph.subjects(RDF.type, RDF.Property):
            if isinstance(prop_uri, URIRef):
                prop_info = self._extract_property_info(prop_uri)
                self.properties[str(prop_uri)] = prop_info
        
        # Also check for OWL object and datatype properties
        for prop_uri in self.graph.subjects(RDF.type, OWL.ObjectProperty):
            if isinstance(prop_uri, URIRef) and str(prop_uri) not in self.properties:
                prop_info = self._extract_property_info(prop_uri)
                prop_info["type"] = "owl:ObjectProperty"
                self.properties[str(prop_uri)] = prop_info
                
        for prop_uri in self.graph.subjects(RDF.type, OWL.DatatypeProperty):
            if isinstance(prop_uri, URIRef) and str(prop_uri) not in self.properties:
                prop_info = self._extract_property_info(prop_uri)
                prop_info["type"] = "owl:DatatypeProperty"
                self.properties[str(prop_uri)] = prop_info
        
        # Index a sample of instances (for performance reasons)
        instance_count = 0
        max_instances = 10000  # Limit the number of instances to index
        
        # Find instances of each class
        for class_uri in self.classes:
            for instance_uri in self.graph.subjects(RDF.type, URIRef(class_uri)):
                if isinstance(instance_uri, URIRef) and str(instance_uri) not in self.instances:
                    instance_info = self._extract_instance_info(instance_uri)
                    self.instances[str(instance_uri)] = instance_info
                    instance_count += 1
                    
                    if instance_count >= max_instances:
                        break
            
            if instance_count >= max_instances:
                break
    
    def _extract_class_info(self, class_uri: URIRef) -> Dict[str, Any]:
        """
        Extract information about a class.
        
        Args:
            class_uri: URI of the class
            
        Returns:
            Dictionary with class information
        """
        info = {
            "uri": str(class_uri),
            "label": self._get_label(class_uri),
            "comment": self._get_comment(class_uri),
            "superclasses": [],
            "subclasses": [],
            "properties": []
        }
        
        # Get superclasses
        for superclass in self.graph.objects(class_uri, RDFS.subClassOf):
            if isinstance(superclass, URIRef):
                info["superclasses"].append(str(superclass))
        
        # Get subclasses
        for subclass in self.graph.subjects(RDFS.subClassOf, class_uri):
            if isinstance(subclass, URIRef):
                info["subclasses"].append(str(subclass))
        
        # Get properties that have this class as domain
        for prop in self.graph.subjects(RDFS.domain, class_uri):
            if isinstance(prop, URIRef):
                info["properties"].append(str(prop))
        
        return info
    
    def _extract_property_info(self, prop_uri: URIRef) -> Dict[str, Any]:
        """
        Extract information about a property.
        
        Args:
            prop_uri: URI of the property
            
        Returns:
            Dictionary with property information
        """
        # Determine property type
        prop_type = "rdf:Property"
        if (prop_uri, RDF.type, OWL.ObjectProperty) in self.graph:
            prop_type = "owl:ObjectProperty"
        elif (prop_uri, RDF.type, OWL.DatatypeProperty) in self.graph:
            prop_type = "owl:DatatypeProperty"
        
        info = {
            "uri": str(prop_uri),
            "label": self._get_label(prop_uri),
            "comment": self._get_comment(prop_uri),
            "type": prop_type,
            "domains": [],
            "ranges": []
        }
        
        # Get domains
        for domain in self.graph.objects(prop_uri, RDFS.domain):
            if isinstance(domain, URIRef):
                info["domains"].append(str(domain))
        
        # Get ranges
        for range_uri in self.graph.objects(prop_uri, RDFS.range):
            if isinstance(range_uri, URIRef):
                info["ranges"].append(str(range_uri))
        
        return info
    
    def _extract_instance_info(self, instance_uri: URIRef) -> Dict[str, Any]:
        """
        Extract information about an instance.
        
        Args:
            instance_uri: URI of the instance
            
        Returns:
            Dictionary with instance information
        """
        info = {
            "uri": str(instance_uri),
            "label": self._get_label(instance_uri),
            "types": [],
            "properties": {}
        }
        
        # Get types
        for type_uri in self.graph.objects(instance_uri, RDF.type):
            if isinstance(type_uri, URIRef):
                info["types"].append(str(type_uri))
        
        # Get property values (limit to common properties for performance)
        for prop, obj in self.graph.predicate_objects(instance_uri):
            if isinstance(prop, URIRef) and str(prop) not in [str(RDF.type)]:
                prop_str = str(prop)
                
                if prop_str not in info["properties"]:
                    info["properties"][prop_str] = []
                
                # Format the object value
                if isinstance(obj, URIRef):
                    value = {
                        "type": "uri",
                        "value": str(obj),
                        "label": self._get_label(obj)
                    }
                elif isinstance(obj, Literal):
                    value = {
                        "type": "literal",
                        "value": str(obj),
                        "datatype": str(obj.datatype) if obj.datatype else None
                    }
                else:
                    value = {
                        "type": "unknown",
                        "value": str(obj)
                    }
                
                info["properties"][prop_str].append(value)
        
        return info
    
    def _get_label(self, uri: URIRef) -> str:
        """Get the label for a URI."""
        labels = list(self.graph.objects(uri, RDFS.label))
        if labels:
            # Prefer English labels
            for label in labels:
                if isinstance(label, Literal) and label.language == "en":
                    return str(label)
            # Fall back to any label
            return str(labels[0])
        
        # No label, use the URI fragment or last path segment
        uri_str = str(uri)
        if "#" in uri_str:
            return uri_str.split("#")[-1]
        else:
            return uri_str.split("/")[-1]
    
    def _get_comment(self, uri: URIRef) -> Optional[str]:
        """Get the comment for a URI."""
        comments = list(self.graph.objects(uri, RDFS.comment))
        if comments:
            # Prefer English comments
            for comment in comments:
                if isinstance(comment, Literal) and comment.language == "en":
                    return str(comment)
            # Fall back to any comment
            return str(comments[0])
        return None
    
    def _update_stats(self):
        """Update ontology statistics."""
        self.stats["total_triples"] = len(self.graph)
        self.stats["class_count"] = len(self.classes)
        self.stats["property_count"] = len(self.properties)
        self.stats["instance_count"] = len(self.instances)
    
    @cache_result()
    def _query_graphdb(self, query: str) -> pd.DataFrame:
        """
        Execute a SPARQL query against GraphDB and return results as a DataFrame.
        
        Args:
            query: SPARQL query string
            
        Returns:
            pandas.DataFrame: Query results
        """
        # Add prefixes if not already in the query
        if not re.search(r'PREFIX\s+', query, re.IGNORECASE):
            prefix_str = ""
            for prefix, uri in self.prefixes.items():
                prefix_str += f"PREFIX {prefix}: <{uri}>\n"
            query = prefix_str + query
        
        self.sparql.setQuery(query)
        
        try:
            results = self.sparql.query().convert()
            
            # Process results
            variables = results['head']['vars']
            bindings = results['results']['bindings']
            
            # Convert results to a DataFrame
            rows = []
            for binding in bindings:
                row = {}
                for var in variables:
                    if var in binding:
                        row[var] = binding[var]['value']
                    else:
                        row[var] = None
                rows.append(row)
            
            return pd.DataFrame(rows)
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def search_classes(
        self, 
        query: str, 
        limit: int = 10, 
        threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Search for classes matching a query.
        
        Args:
            query: Search query
            limit: Maximum number of results
            threshold: Minimum similarity threshold
            
        Returns:
            List of matching classes with similarity scores
        """
        # Query GraphDB directly
        sparql_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?class ?label ?comment
        WHERE {{
            ?class a owl:Class .
            
            {{
                ?class rdfs:label ?label .
                FILTER(CONTAINS(LCASE(?label), LCASE("{query}")))
            }} UNION {{
                BIND(REPLACE(STR(?class), "^.*/([^/#]*)$|^.*#([^/#]*)$", "$1$2") AS ?localName)
                FILTER(CONTAINS(LCASE(?localName), LCASE("{query}")))
                OPTIONAL {{ ?class rdfs:label ?label }}
            }}
            
            OPTIONAL {{ ?class rdfs:comment ?comment }}
        }}
        LIMIT {limit}
        """
        
        df = self._query_graphdb(sparql_query)
        
        results = []
        for _, row in df.iterrows():
            # Determine label and URI
            class_uri = row['class']
            label = row.get('label', self._extract_name_from_uri(class_uri))
            
            # Calculate similarity score
            similarity = 1.0 if query.lower() in label.lower() else 0.8
            
            if similarity >= threshold:
                results.append({
                    "uri": class_uri,
                    "label": label,
                    "comment": row.get('comment'),
                    "similarity": similarity,
                    "superclasses": [],  # Can fetch these on demand if needed
                    "subclasses": []
                })
        
        return results
    
    def search_properties(
        self, 
        query: str, 
        limit: int = 10, 
        threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Search for properties matching a query.
        
        Args:
            query: Search query
            limit: Maximum number of results
            threshold: Minimum similarity threshold
            
        Returns:
            List of matching properties with similarity scores
        """
        # Query GraphDB directly
        sparql_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?property ?type ?label ?comment
        WHERE {{
            {{
                ?property a rdf:Property .
                BIND("rdf:Property" AS ?type)
            }} UNION {{
                ?property a owl:ObjectProperty .
                BIND("owl:ObjectProperty" AS ?type)
            }} UNION {{
                ?property a owl:DatatypeProperty .
                BIND("owl:DatatypeProperty" AS ?type)
            }}
            
            {{
                ?property rdfs:label ?label .
                FILTER(CONTAINS(LCASE(?label), LCASE("{query}")))
            }} UNION {{
                BIND(REPLACE(STR(?property), "^.*/([^/#]*)$|^.*#([^/#]*)$", "$1$2") AS ?localName)
                FILTER(CONTAINS(LCASE(?localName), LCASE("{query}")))
                OPTIONAL {{ ?property rdfs:label ?label }}
            }}
            
            OPTIONAL {{ ?property rdfs:comment ?comment }}
        }}
        LIMIT {limit}
        """
        
        df = self._query_graphdb(sparql_query)
        
        results = []
        for _, row in df.iterrows():
            # Determine label and URI
            property_uri = row['property']
            label = row.get('label', self._extract_name_from_uri(property_uri))
            
            # Calculate similarity score
            similarity = 1.0 if query.lower() in label.lower() else 0.8
            
            if similarity >= threshold:
                results.append({
                    "uri": property_uri,
                    "label": label,
                    "comment": row.get('comment'),
                    "type": row.get('type', 'rdf:Property'),
                    "similarity": similarity,
                    "domains": [],  # Can fetch these on demand if needed
                    "ranges": []
                })
        
        return results
    
    def search_instances(
        self, 
        query: str, 
        limit: int = 10, 
        threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Search for instances matching a query.
        
        Args:
            query: Search query
            limit: Maximum number of results
            threshold: Minimum similarity threshold
            
        Returns:
            List of matching instances with similarity scores
        """
        # Query GraphDB directly
        sparql_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?instance ?type ?label
        WHERE {{
            ?instance a ?type .
            ?type a owl:Class .
            
            {{
                ?instance rdfs:label ?label .
                FILTER(CONTAINS(LCASE(?label), LCASE("{query}")))
            }} UNION {{
                BIND(REPLACE(STR(?instance), "^.*/([^/#]*)$|^.*#([^/#]*)$", "$1$2") AS ?localName)
                FILTER(CONTAINS(LCASE(?localName), LCASE("{query}")))
                OPTIONAL {{ ?instance rdfs:label ?label }}
            }}
        }}
        LIMIT {limit}
        """
        
        df = self._query_graphdb(sparql_query)
        
        results = []
        for _, row in df.iterrows():
            # Determine label and URI
            instance_uri = row['instance']
            type_uri = row['type']
            label = row.get('label', self._extract_name_from_uri(instance_uri))
            
            # Calculate similarity score
            similarity = 1.0 if query.lower() in label.lower() else 0.8
            
            if similarity >= threshold:
                results.append({
                    "uri": instance_uri,
                    "label": label,
                    "types": [type_uri],
                    "similarity": similarity
                })
        
        return results
    
    def _extract_name_from_uri(self, uri: str) -> str:
        """Extract a readable label from a URI."""
        if "#" in uri:
            return uri.split("#")[-1]
        else:
            return uri.split("/")[-1]
    
    def get_class(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about a class by URI."""
        if uri in self.classes:
            return self.classes[uri]
        
        # Query GraphDB directly
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?label ?comment ?superClass
        WHERE {{
            OPTIONAL {{ <{uri}> rdfs:label ?label }}
            OPTIONAL {{ <{uri}> rdfs:comment ?comment }}
            OPTIONAL {{ <{uri}> rdfs:subClassOf ?superClass }}
        }}
        """
        
        df = self._query_graphdb(query)
        
        if df.empty:
            return None
        
        class_info = {
            "uri": uri,
            "label": df['label'].iloc[0] if 'label' in df and not pd.isna(df['label'].iloc[0]) else self._extract_name_from_uri(uri),
            "comment": df['comment'].iloc[0] if 'comment' in df and not pd.isna(df['comment'].iloc[0]) else None,
            "superclasses": df['superClass'].dropna().tolist() if 'superClass' in df else [],
            "subclasses": [],
            "properties": []
        }
        
        # Cache the result
        self.classes[uri] = class_info
        
        return class_info
    
    def get_property(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about a property by URI."""
        if uri in self.properties:
            return self.properties[uri]
        
        # Query GraphDB directly
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?label ?comment ?domain ?range ?type
        WHERE {{
            OPTIONAL {{ <{uri}> rdfs:label ?label }}
            OPTIONAL {{ <{uri}> rdfs:comment ?comment }}
            OPTIONAL {{ <{uri}> rdfs:domain ?domain }}
            OPTIONAL {{ <{uri}> rdfs:range ?range }}
            OPTIONAL {{ 
                <{uri}> rdf:type ?type .
                FILTER(?type IN (rdf:Property, owl:ObjectProperty, owl:DatatypeProperty, owl:AnnotationProperty))
            }}
        }}
        """
        
        df = self._query_graphdb(query)
        
        if df.empty:
            return None
        
        # Determine property type
        prop_type = "rdf:Property"
        if 'type' in df and not df['type'].empty:
            types = df['type'].dropna().unique()
            if any('ObjectProperty' in t for t in types):
                prop_type = "owl:ObjectProperty"
            elif any('DatatypeProperty' in t for t in types):
                prop_type = "owl:DatatypeProperty"
            elif any('AnnotationProperty' in t for t in types):
                prop_type = "owl:AnnotationProperty"
        
        property_info = {
            "uri": uri,
            "label": df['label'].iloc[0] if 'label' in df and not pd.isna(df['label'].iloc[0]) else self._extract_name_from_uri(uri),
            "comment": df['comment'].iloc[0] if 'comment' in df and not pd.isna(df['comment'].iloc[0]) else None,
            "type": prop_type,
            "domains": df['domain'].dropna().tolist() if 'domain' in df else [],
            "ranges": df['range'].dropna().tolist() if 'range' in df else []
        }
        
        # Cache the result
        self.properties[uri] = property_info
        
        return property_info
    
    def get_instance(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about an instance by URI."""
        if uri in self.instances:
            return self.instances[uri]
        
        # Query GraphDB directly for basic info
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?type ?label
        WHERE {{
            <{uri}> rdf:type ?type .
            OPTIONAL {{ <{uri}> rdfs:label ?label }}
            
            # Only return class types, not rdf:type owl:NamedIndividual etc.
            ?type a ?classType .
            FILTER(?classType IN (owl:Class, rdfs:Class))
        }}
        """
        
        df = self._query_graphdb(query)
        
        if df.empty:
            return None
        
        instance_info = {
            "uri": uri,
            "label": df['label'].iloc[0] if 'label' in df and not pd.isna(df['label'].iloc[0]) else self._extract_name_from_uri(uri),
            "types": df['type'].dropna().tolist() if 'type' in df else [],
            "properties": {}
        }
        
        # Get properties (optional - can be expensive)
        prop_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT ?property ?value
        WHERE {{
            <{uri}> ?property ?value .
            FILTER(?property != rdf:type)
        }}
        LIMIT 100
        """
        
        prop_df = self._query_graphdb(prop_query)
        
        if not prop_df.empty:
            for _, row in prop_df.iterrows():
                prop_uri = row['property']
                value = row['value']
                
                if prop_uri not in instance_info["properties"]:
                    instance_info["properties"][prop_uri] = []
                
                # Format the value
                if value.startswith('http://') or value.startswith('https://'):
                    # URI value
                    formatted_value = {
                        "type": "uri",
                        "value": value,
                        "label": self._extract_name_from_uri(value)
                    }
                else:
                    # Literal value
                    formatted_value = {
                        "type": "literal",
                        "value": value,
                        "datatype": None  # Cannot determine from simple SPARQL results
                    }
                
                instance_info["properties"][prop_uri].append(formatted_value)
        
        # Cache the result
        self.instances[uri] = instance_info
        
        return instance_info
    
    def get_ontology_summary(self) -> Dict[str, Any]:
        """Get a summary of the ontology."""
        # Query for top-level classes (those without a superclass or only owl:Thing as superclass)
        top_classes_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?class ?label ?comment (COUNT(?subClass) AS ?subClassCount)
        WHERE {
            ?class a owl:Class .
            OPTIONAL { ?class rdfs:label ?label }
            OPTIONAL { ?class rdfs:comment ?comment }
            
            # Count subclasses for each class
            OPTIONAL { ?subClass rdfs:subClassOf ?class }
            
            # Only get classes that have no superclass or only Thing as superclass
            FILTER NOT EXISTS { 
                ?class rdfs:subClassOf ?superClass . 
                FILTER(?superClass != owl:Thing && ?superClass != rdfs:Resource)
            }
        }
        GROUP BY ?class ?label ?comment
        ORDER BY DESC(?subClassCount)
        LIMIT 10
        """
        
        top_classes_df = self._query_graphdb(top_classes_query)
        
        # Query for property types
        property_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?property ?type ?label ?comment
        WHERE {
            {
                ?property a owl:ObjectProperty .
                BIND("owl:ObjectProperty" AS ?type)
            } UNION {
                ?property a owl:DatatypeProperty .
                BIND("owl:DatatypeProperty" AS ?type)
            } UNION {
                ?property a rdf:Property .
                FILTER NOT EXISTS { ?property a owl:ObjectProperty }
                FILTER NOT EXISTS { ?property a owl:DatatypeProperty }
                BIND("rdf:Property" AS ?type)
            }
            OPTIONAL { ?property rdfs:label ?label }
            OPTIONAL { ?property rdfs:comment ?comment }
        }
        LIMIT 30
        """
        
        property_df = self._query_graphdb(property_query)
        
        # Format results
        top_classes = []
        for _, row in top_classes_df.iterrows():
            top_classes.append({
                "uri": row["class"],
                "label": row.get("label", self._extract_name_from_uri(row["class"])),
                "comment": row.get("comment"),
                "subclasses": int(row["subClassCount"]) if "subClassCount" in row else 0
            })
        
        obj_properties = []
        data_properties = []
        other_properties = []
        
        for _, row in property_df.iterrows():
            prop_info = {
                "uri": row["property"],
                "label": row.get("label", self._extract_name_from_uri(row["property"])),
                "comment": row.get("comment")
            }
            
            prop_type = row.get("type", "rdf:Property")
            if prop_type == "owl:ObjectProperty":
                obj_properties.append(prop_info)
            elif prop_type == "owl:DatatypeProperty":
                data_properties.append(prop_info)
            else:
                other_properties.append(prop_info)
        
        return {
            "stats": self.stats,
            "top_classes": top_classes,
            "object_properties": obj_properties[:10],
            "datatype_properties": data_properties[:10],
            "other_properties": other_properties[:10]
        }
    
    def execute_sparql(self, query: str) -> Dict[str, Any]:
        """
        Execute a SPARQL query against the GraphDB endpoint.
        
        Args:
            query: SPARQL query string
            
        Returns:
            Query results
        """
        try:
            # Add prefixes if not already in the query
            if not re.search(r'PREFIX\s+', query, re.IGNORECASE):
                prefix_str = ""
                for prefix, uri in self.prefixes.items():
                    prefix_str += f"PREFIX {prefix}: <{uri}>\n"
                query = prefix_str + query
            
            # Determine query type
            query_upper = query.strip().upper()
            
            if query_upper.startswith("ASK"):
                # ASK query
                self.sparql.setQuery(query)
                results = self.sparql.query().convert()
                
                return {
                    "success": True,
                    "type": "boolean",
                    "value": results.get("boolean", False)
                }
            
            elif query_upper.startswith("SELECT"):
                # SELECT query - convert to standard format
                df = self._query_graphdb(query)
                
                # Convert DataFrame to expected format
                bindings = []
                for _, row in df.iterrows():
                    binding = {}
                    for col in df.columns:
                        if not pd.isna(row[col]):
                            # Infer type
                            value = row[col]
                            if isinstance(value, str) and (value.startswith('http://') or value.startswith('https://')):
                                binding[col] = {
                                    "type": "uri",
                                    "value": value
                                }
                            else:
                                binding[col] = {
                                    "type": "literal",
                                    "value": str(value)
                                }
                    bindings.append(binding)
                
                return {
                    "success": True,
                    "type": "bindings",
                    "head": {
                        "vars": list(df.columns)
                    },
                    "results": {
                        "bindings": bindings
                    }
                }
            
            elif query_upper.startswith("CONSTRUCT") or query_upper.startswith("DESCRIBE"):
                # For CONSTRUCT and DESCRIBE, just pass-through the results
                self.sparql.setQuery(query)
                results = self.sparql.query().convert()
                
                return {
                    "success": True,
                    "type": "graph",
                    "results": results
                }
            
            else:
                return {
                    "success": False,
                    "error": "Unsupported query type"
                }
                
        except Exception as e:
            logger.error(f"Error executing SPARQL query: {e}")
            return {
                "success": False,
                "error": str(e)
            }
