import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS, XSD

logger = logging.getLogger(__name__)

class OntologyStore:
    """
    Store for managing access to ontology data.
    Handles loading, querying, and caching of ontology information.
    """
    
    def __init__(
        self, 
        local_path: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        prefixes: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the ontology store.
        
        Args:
            local_path: Path to local ontology file (RDF/OWL/TTL)
            endpoint_url: URL of SPARQL endpoint for remote ontology
            prefixes: Dictionary of namespace prefixes
        """
        self.local_path = local_path
        self.endpoint_url = endpoint_url
        
        # Initialize RDF graph
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
        
        # Try loading from local file first
        if self.local_path and os.path.exists(self.local_path):
            success = self._load_from_file()
        
        # If local loading failed or no file specified, try endpoint
        if not success and self.endpoint_url:
            success = self._load_from_endpoint()
        
        # If either loading method was successful, build indices
        if success:
            self._build_indices()
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
    
    def _load_from_endpoint(self) -> bool:
        """
        Load ontology from a SPARQL endpoint.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading ontology from endpoint: {self.endpoint_url}")
            
            # Use SPARQLWrapper to query the endpoint
            from SPARQLWrapper import RDFXML, SPARQLWrapper
            
            sparql = SPARQLWrapper(self.endpoint_url)
            
            # Get core ontology structure with a CONSTRUCT query
            query = """
            CONSTRUCT {
                ?class a owl:Class ;
                       rdfs:subClassOf ?superClass ;
                       rdfs:label ?classLabel ;
                       rdfs:comment ?classComment .
                
                ?property a rdf:Property ;
                          rdfs:domain ?domain ;
                          rdfs:range ?range ;
                          rdfs:label ?propLabel ;
                          rdfs:comment ?propComment .
            }
            WHERE {
                {
                    ?class a owl:Class .
                    OPTIONAL { ?class rdfs:subClassOf ?superClass . }
                    OPTIONAL { ?class rdfs:label ?classLabel . }
                    OPTIONAL { ?class rdfs:comment ?classComment . }
                } UNION {
                    ?property a rdf:Property .
                    OPTIONAL { ?property rdfs:domain ?domain . }
                    OPTIONAL { ?property rdfs:range ?range . }
                    OPTIONAL { ?property rdfs:label ?propLabel . }
                    OPTIONAL { ?property rdfs:comment ?propComment . }
                }
            }
            LIMIT 10000
            """
            
            sparql.setQuery(query)
            sparql.setReturnFormat(RDFXML)
            results = sparql.query().convert()
            
            # Parse results into the graph
            if results:
                self.graph.parse(data=results, format="xml")
            
            return len(self.graph) > 0
        except Exception as e:
            logger.error(f"Error loading ontology from endpoint: {e}")
            return False
    
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
        results = []
        query_lower = query.lower()
        
        for uri, info in self.classes.items():
            similarity = 0.0
            label = info["label"].lower()
            
            # Check for exact match
            if query_lower == label:
                similarity = 1.0
            # Check for substring match
            elif query_lower in label:
                similarity = 0.8
            # Check for word match
            elif any(q_word in label.split() for q_word in query_lower.split()):
                similarity = 0.6
            
            if similarity >= threshold:
                results.append({
                    **info,
                    "similarity": similarity
                })
        
        # Sort by similarity
        results.sort(key=lambda x: x["similarity"], reverse=True)
        
        return results[:limit]
    
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
        results = []
        query_lower = query.lower()
        
        for uri, info in self.properties.items():
            similarity = 0.0
            label = info["label"].lower()
            
            # Check for exact match
            if query_lower == label:
                similarity = 1.0
            # Check for substring match
            elif query_lower in label:
                similarity = 0.8
            # Check for word match
            elif any(q_word in label.split() for q_word in query_lower.split()):
                similarity = 0.6
            
            if similarity >= threshold:
                results.append({
                    **info,
                    "similarity": similarity
                })
        
        # Sort by similarity
        results.sort(key=lambda x: x["similarity"], reverse=True)
        
        return results[:limit]
    
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
        results = []
        query_lower = query.lower()
        
        for uri, info in self.instances.items():
            similarity = 0.0
            label = info["label"].lower()
            
            # Check for exact match
            if query_lower == label:
                similarity = 1.0
            # Check for substring match
            elif query_lower in label:
                similarity = 0.8
            # Check for word match
            elif any(q_word in label.split() for q_word in query_lower.split()):
                similarity = 0.6
            
            if similarity >= threshold:
                results.append({
                    **info,
                    "similarity": similarity
                })
        
        # Sort by similarity
        results.sort(key=lambda x: x["similarity"], reverse=True)
        
        return results[:limit]
    
    def get_class(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about a class by URI."""
        return self.classes.get(uri)
    
    def get_property(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about a property by URI."""
        return self.properties.get(uri)
    
    def get_instance(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get information about an instance by URI."""
        return self.instances.get(uri)
    
    def get_ontology_summary(self) -> Dict[str, Any]:
        """Get a summary of the ontology."""
        # Get top-level classes (those without superclasses or only owl:Thing as superclass)
        top_classes = []
        for uri, info in self.classes.items():
            if not info["superclasses"] or all(sc == str(OWL.Thing) for sc in info["superclasses"]):
                top_classes.append(info)
        
        # Get top-level properties grouped by type
        obj_properties = []
        data_properties = []
        other_properties = []
        
        for uri, info in self.properties.items():
            if info["type"] == "owl:ObjectProperty":
                obj_properties.append(info)
            elif info["type"] == "owl:DatatypeProperty":
                data_properties.append(info)
            else:
                other_properties.append(info)
        
        # Limit to top 10 of each for the summary
        return {
            "stats": self.stats,
            "top_classes": sorted(top_classes, key=lambda x: len(x["subclasses"]), reverse=True)[:10],
            "object_properties": sorted(obj_properties, key=lambda x: x["label"])[:10],
            "datatype_properties": sorted(data_properties, key=lambda x: x["label"])[:10],
            "other_properties": sorted(other_properties, key=lambda x: x["label"])[:10]
        }
    
    def execute_sparql(self, query: str) -> Dict[str, Any]:
        """
        Execute a SPARQL query against the local graph.
        
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
            
            # Execute the query
            results = self.graph.query(query)
            
            # Format results based on query type
            if query.strip().upper().startswith("ASK"):
                return {
                    "success": True,
                    "type": "boolean",
                    "value": bool(results)
                }
            elif query.strip().upper().startswith("SELECT"):
                # Format SELECT results
                bindings = []
                for row in results:
                    binding = {}
                    for var, value in zip(results.vars, row):
                        if isinstance(value, URIRef):
                            binding[str(var)] = {
                                "type": "uri",
                                "value": str(value),
                                "label": self._get_label(value)
                            }
                        elif isinstance(value, Literal):
                            binding[str(var)] = {
                                "type": "literal",
                                "value": str(value),
                                "datatype": str(value.datatype) if value.datatype else None
                            }
                        elif isinstance(value, BNode):
                            binding[str(var)] = {
                                "type": "bnode",
                                "value": str(value)
                            }
                        else:
                            binding[str(var)] = {
                                "type": "unknown",
                                "value": str(value)
                            }
                    bindings.append(binding)
                
                return {
                    "success": True,
                    "type": "bindings",
                    "head": {
                        "vars": [str(var) for var in results.vars]
                    },
                    "results": {
                        "bindings": bindings
                    }
                }
            elif query.strip().upper().startswith(("CONSTRUCT", "DESCRIBE")):
                # Format CONSTRUCT/DESCRIBE results (triples)
                triples = []
                for s, p, o in results:
                    triple = {
                        "subject": {
                            "type": "uri" if isinstance(s, URIRef) else "bnode",
                            "value": str(s),
                            "label": self._get_label(s) if isinstance(s, URIRef) else None
                        },
                        "predicate": {
                            "type": "uri",
                            "value": str(p),
                            "label": self._get_label(p)
                        }
                    }
                    
                    if isinstance(o, URIRef):
                        triple["object"] = {
                            "type": "uri",
                            "value": str(o),
                            "label": self._get_label(o)
                        }
                    elif isinstance(o, Literal):
                        triple["object"] = {
                            "type": "literal",
                            "value": str(o),
                            "datatype": str(o.datatype) if o.datatype else None
                        }
                    elif isinstance(o, BNode):
                        triple["object"] = {
                            "type": "bnode",
                            "value": str(o)
                        }
                    else:
                        triple["object"] = {
                            "type": "unknown",
                            "value": str(o)
                        }
                    
                    triples.append(triple)
                
                return {
                    "success": True,
                    "type": "triples",
                    "triples": triples
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
