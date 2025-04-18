import json
from typing import Any, Dict, List, Optional

import autogen
import numpy as np
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS
from sentence_transformers import SentenceTransformer

from config.agent_config import OPEN_API_KEY
from database.ontology_store import OntologyStore


class OntologyMappingAgent:
    """
    Slave agent responsible for mapping natural language entities to ontology terms.
    Resolves ambiguities and understands class hierarchies and property relationships.
    """
    
    def __init__(
        self, 
        ontology_path: Optional[str] = None,
        ontology_endpoint: Optional[str] = None,
        embedding_model_name: str = "all-MiniLM-L6-v2",
        ontology_store: Optional[OntologyStore] = None  # Add this parameter
    ):
        """
        Initialize the ontology mapping agent.
        
        Args:
            ontology_path: Local path to ontology file (RDF/OWL/TTL)
            ontology_endpoint: SPARQL endpoint for remote ontology access
            embedding_model_name: Name of the embedding model for semantic matching
            ontology_store: Optional pre-initialized ontology store object
        """
        # Initialize ontology graph
        self.graph = Graph()
        
        # Use provided ontology_store if available, otherwise create new one
        if ontology_store:
            self.graph = ontology_store.graph
            self.local_path = ontology_store.local_path
            self.endpoint_url = ontology_store.endpoint_url
            # Reuse the ontology store's cached data
            self.class_hierarchy = ontology_store.classes
            self.property_domains_ranges = ontology_store.properties
            self.instances = ontology_store.instances
        else:
            # Load ontology as before
            self.local_path = ontology_path
            self.endpoint_url = ontology_endpoint
            
            if ontology_path:
                self._load_local_ontology(ontology_path)
            elif ontology_endpoint:
                self._load_remote_ontology(ontology_endpoint)
                
            # Build indices
            self.class_hierarchy = self._build_class_hierarchy()
            self.property_domains_ranges = self._build_property_domains_ranges()
        
        # Initialize embedding model for semantic matching
        self.embedding_model = SentenceTransformer(embedding_model_name)
        
        # Cache for ontology term embeddings
        self.term_embeddings = {}
        
        # Cache for ontology structure
        self.class_hierarchy = self._build_class_hierarchy()
        self.property_domains_ranges = self._build_property_domains_ranges()
        
        # Initialize LLM agent for complex mappings
        agent_config = {
            "name": "OntologyMappingAssistant",
            "system_message": """You are an ontology mapping specialist. 
Your task is to map natural language terms to formal ontology terms.
Analyze the context, term descriptions, and ontology structure to find the best matches.""",
            "llm_config": {
                "config_list": [{"model": "gpt-4o-mini", "api_key": OPEN_API_KEY}],
                "temperature": 0.0
            }
        }
        
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        self.proxy = autogen.UserProxyAgent(
            name="OntologyMappingProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
    
    def map_entities(
        self, 
        entities: Dict[str, Any], 
        query_context: str
    ) -> Dict[str, Any]:
        """
        Map extracted entities to ontology terms.
        
        Args:
            entities: Dictionary of extracted entities with types
            query_context: The full query for context
            
        Returns:
            Dictionary of mapped ontology terms
        """
        mapped_entities = {
            "classes": [],
            "properties": [],
            "instances": [],
            "literals": [],
            "unknown": []
        }
        
        # Process each entity by type
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                entity_text = entity.get("text", "")
                if not entity_text:
                    continue
                
                # Map entity based on its type
                if entity_type == "CLASS":
                    mapped_class = self._map_to_class(entity_text, query_context)
                    if mapped_class:
                        mapped_entities["classes"].append(mapped_class)
                    else:
                        mapped_entities["unknown"].append({"text": entity_text, "type": "CLASS"})
                
                elif entity_type == "PROPERTY":
                    mapped_property = self._map_to_property(entity_text, query_context)
                    if mapped_property:
                        mapped_entities["properties"].append(mapped_property)
                    else:
                        mapped_entities["unknown"].append({"text": entity_text, "type": "PROPERTY"})
                
                elif entity_type == "INSTANCE":
                    mapped_instance = self._map_to_instance(entity_text, query_context)
                    if mapped_instance:
                        mapped_entities["instances"].append(mapped_instance)
                    else:
                        mapped_entities["unknown"].append({"text": entity_text, "type": "INSTANCE"})
                
                elif entity_type == "LITERAL":
                    # Literals typically don't need mapping, just type inference
                    literal_type = self._infer_literal_type(entity_text)
                    mapped_entities["literals"].append({
                        "text": entity_text,
                        "inferred_type": literal_type
                    })
                else:
                    # Unknown entity type, try general mapping
                    mapped_term = self._general_term_mapping(entity_text, query_context)
                    if mapped_term:
                        mapped_category = mapped_term.pop("category")
                        mapped_entities[mapped_category].append(mapped_term)
                    else:
                        mapped_entities["unknown"].append({"text": entity_text, "type": "UNKNOWN"})
        
        # For entities that couldn't be mapped automatically, use LLM-based mapping.
        if mapped_entities["unknown"]:
            llm_mapped = self._llm_based_mapping(mapped_entities["unknown"], query_context, mapped_entities)
            # Update mapped entities with LLM results
            for category, items in llm_mapped.items():
                if category != "unknown":
                    mapped_entities[category].extend(items)
            
            # Update unknown list to only contain truly unmapped entities.
            mapped_entities["unknown"] = llm_mapped.get("unknown", [])
        return mapped_entities
    
    def _load_local_ontology(self, ontology_path: str):
        """Load ontology from a local file."""
        try:
            self.graph.parse(ontology_path)
            print(f"Loaded ontology with {len(self.graph)} triples")
        except Exception as e:
            print(f"Error loading ontology: {e}")
    
    def _load_remote_ontology(self, endpoint_url: str):
        """Load ontology from a remote SPARQL endpoint."""
        try:
            # SPARQL query to get basic ontology structure
            query = """
            CONSTRUCT {
                ?class a owl:Class.
                ?class rdfs:subClassOf ?superClass.
                ?class rdfs:label ?classLabel.
                ?class rdfs:comment ?classComment.
                
                ?property a rdf:Property.
                ?property rdfs:domain ?domain.
                ?property rdfs:range ?range.
                ?property rdfs:label ?propLabel.
                ?property rdfs:comment ?propComment.
            }
            WHERE {
                {
                    ?class a owl:Class.
                    OPTIONAL { ?class rdfs:subClassOf ?superClass. }
                    OPTIONAL { ?class rdfs:label ?classLabel. }
                    OPTIONAL { ?class rdfs:comment ?classComment. }
                } UNION {
                    ?property a rdf:Property.
                    OPTIONAL { ?property rdfs:domain ?domain. }
                    OPTIONAL { ?property rdfs:range ?range. }
                    OPTIONAL { ?property rdfs:label ?propLabel. }
                    OPTIONAL { ?property rdfs:comment ?propComment. }
                }
            }
            """
            
            # Set up SPARQL endpoint and execute query
            from SPARQLWrapper import JSON, SPARQLWrapper
            sparql = SPARQLWrapper(endpoint_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            
            # Parse results into our graph
            # This would depend on the format returned by the endpoint
            # For simplicity, assuming it can be directly parsed
            self.graph.parse(data=results)
            
            print(f"Loaded ontology from endpoint with {len(self.graph)} triples")
        except Exception as e:
            print(f"Error loading ontology from endpoint: {e}")
    
    def _build_class_hierarchy(self) -> Dict[str, Dict[str, Any]]:
        """Build a dictionary representing the class hierarchy."""
        hierarchy = {}
        
        # Get all classes
        for class_uri in self.graph.subjects(RDF.type, OWL.Class):
            if isinstance(class_uri, URIRef):
                class_info = {
                    "uri": str(class_uri),
                    "labels": list(self.graph.objects(class_uri, RDFS.label)),
                    "comments": list(self.graph.objects(class_uri, RDFS.comment)),
                    "superclasses": list(self.graph.objects(class_uri, RDFS.subClassOf)),
                    "subclasses": list(self.graph.subjects(RDFS.subClassOf, class_uri))
                }
                
                # Convert URIRefs to strings
                class_info["superclasses"] = [str(sc) for sc in class_info["superclasses"] if isinstance(sc, URIRef)]
                class_info["subclasses"] = [str(sc) for sc in class_info["subclasses"] if isinstance(sc, URIRef)]
                
                # Convert labels and comments to strings
                class_info["labels"] = [str(l) for l in class_info["labels"] if isinstance(l, Literal)]
                class_info["comments"] = [str(c) for c in class_info["comments"] if isinstance(c, Literal)]
                
                # Add to hierarchy
                hierarchy[str(class_uri)] = class_info
        
        return hierarchy
    
    def _build_property_domains_ranges(self) -> Dict[str, Dict[str, Any]]:
        """Build a dictionary of properties with their domains and ranges."""
        properties = {}
        
        # Get all properties (both object and datatype)
        property_types = [RDF.Property, OWL.ObjectProperty, OWL.DatatypeProperty]
        
        for prop_type in property_types:
            for prop_uri in self.graph.subjects(RDF.type, prop_type):
                if isinstance(prop_uri, URIRef):
                    prop_info = {
                        "uri": str(prop_uri),
                        "type": str(prop_type),
                        "labels": list(self.graph.objects(prop_uri, RDFS.label)),
                        "comments": list(self.graph.objects(prop_uri, RDFS.comment)),
                        "domains": list(self.graph.objects(prop_uri, RDFS.domain)),
                        "ranges": list(self.graph.objects(prop_uri, RDFS.range))
                    }
                    
                    # Convert URIRefs to strings
                    prop_info["domains"] = [str(d) for d in prop_info["domains"] if isinstance(d, URIRef)]
                    prop_info["ranges"] = [str(r) for r in prop_info["ranges"] if isinstance(r, URIRef)]
                    
                    # Convert labels and comments to strings
                    prop_info["labels"] = [str(l) for l in prop_info["labels"] if isinstance(l, Literal)]
                    prop_info["comments"] = [str(c) for c in prop_info["comments"] if isinstance(c, Literal)]
                    
                    # Add to properties
                    properties[str(prop_uri)] = prop_info
        
        return properties
    
    def _map_to_class(
        self, 
        entity_text: str, 
        query_context: str
    ) -> Optional[Dict[str, Any]]:
        """
        Map an entity to an ontology class.
        
        Args:
            entity_text: The entity text to map
            query_context: The full query for context
            
        Returns:
            Mapped class information or None if no match
        """
        # First, try exact match with class labels
        for class_uri, class_info in self.class_hierarchy.items():
            for label in class_info["labels"]:
                if entity_text.lower() == label.lower():
                    return {
                        "text": entity_text,
                        "uri": class_uri,
                        "label": label,
                        "type": "class",
                        "confidence": 1.0
                    }
        
        # If no exact match, try semantic matching
        matches = self._semantic_match(entity_text, self.class_hierarchy, "class")
        if matches:
            best_match = matches[0]
            return {
                "text": entity_text,
                "uri": best_match["uri"],
                "label": best_match.get("matched_label", ""),
                "type": "class",
                "confidence": best_match["similarity"]
            }
        return
    
    def _map_to_property(
        self, 
        entity_text: str, 
        query_context: str
    ) -> Optional[Dict[str, Any]]:
        """
        Map an entity to an ontology property.
        
        Args:
            entity_text: The entity text to map
            query_context: The full query for context
            
        Returns:
            Mapped property information or None if no match
        """
        # First, try exact match with property labels
        for prop_uri, prop_info in self.property_domains_ranges.items():
            for label in prop_info["labels"]:
                if entity_text.lower() == label.lower():
                    return {
                        "text": entity_text,
                        "uri": prop_uri,
                        "label": label,
                        "type": "property",
                        "property_type": prop_info["type"],
                        "domains": prop_info["domains"],
                        "ranges": prop_info["ranges"],
                        "confidence": 1.0
                    }
        
        # If no exact match, try semantic matching
        matches = self._semantic_match(entity_text, self.property_domains_ranges, "property")
        if matches:
            best_match = matches[0]
            prop_info = self.property_domains_ranges[best_match["uri"]]
            return {
                "text": entity_text,
                "uri": best_match["uri"],
                "label": best_match.get("matched_label", ""),
                "type": "property",
                "property_type": prop_info["type"],
                "domains": prop_info["domains"],
                "ranges": prop_info["ranges"],
                "confidence": best_match["similarity"]
            }
        return
    
    def _map_to_instance(
        self, 
        entity_text: str, 
        query_context: str
    ) -> Optional[Dict[str, Any]]:
        """
        Map an entity to an ontology instance.
        
        Args:
            entity_text: The entity text to map
            query_context: The full query for context
            
        Returns:
            Mapped instance information or None if no match
        """
        # Find instances in the ontology
        # This is more complex because instances can be numerous
        # and might not be fully loaded in the graph
        
        # For simplicity, we'll use a SPARQL query to find instances with matching labels
        query = f"""
        SELECT ?instance ?label ?type
        WHERE {{
            ?instance rdfs:label ?label .
            ?instance a ?type .
            FILTER(REGEX(STR(?label), "{entity_text}", "i"))
        }}
        """
        
        try:
            # Execute query directly on the graph
            results = []
            qres = self.graph.query(query)
            
            for row in qres:
                instance_uri = row.instance.toPython() if hasattr(row.instance, "toPython") else str(row.instance)
                instance_label = row.label.toPython() if hasattr(row.label, "toPython") else str(row.label)
                instance_type = row.type.toPython() if hasattr(row.type, "toPython") else str(row.type)
                
                # Calculate string similarity
                from difflib import SequenceMatcher
                similarity = SequenceMatcher(None, entity_text.lower(), instance_label.lower()).ratio()
                
                results.append({
                    "uri": instance_uri,
                    "label": instance_label,
                    "type": instance_type,
                    "similarity": similarity
                })
            
            # Sort by similarity
            results.sort(key=lambda x: x["similarity"], reverse=True)
            
            # Return the best match if similarity is above threshold
            if results and results[0]["similarity"] > 0.8:
                best_match = results[0]
                return {
                    "text": entity_text,
                    "uri": best_match["uri"],
                    "label": best_match["label"],
                    "type": "instance",
                    "instance_type": best_match["type"],
                    "confidence": best_match["similarity"]
                }
        
        except Exception as e:
            print(f"Error querying for instances: {e}")
        
        return None
    
    def _infer_literal_type(self, text: str) -> str:
        """Infer the datatype of a literal value."""
        # Try parsing as integer
        try:
            int(text)
            return "xsd:integer"
        except ValueError:
            pass
        
        # Try parsing as float
        try:
            float(text)
            return "xsd:decimal"
        except ValueError:
            pass
        
        # Check if it's a date format (simple check)
        import re
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        if date_pattern.match(text):
            return "xsd:date"
        
        # Check if it's a dateTime format
        datetime_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
        if datetime_pattern.match(text):
            return "xsd:dateTime"
        
        # Default to string
        return "xsd:string"
    
    def _general_term_mapping(
        self, 
        entity_text: str, 
        query_context: str
    ) -> Optional[Dict[str, Any]]:
        """
        Try to map an entity to any type of ontology term.
        
        Args:
            entity_text: The entity text to map
            query_context: The full query for context
            
        Returns:
            Mapped term information with category or None if no match
        """
        # Try mapping to a class
        class_mapping = self._map_to_class(entity_text, query_context)
        if class_mapping and class_mapping.get("confidence", 0) > 0.7:
            class_mapping["category"] = "classes"
            return class_mapping
        
        # Try mapping to a property
        property_mapping = self._map_to_property(entity_text, query_context)
        if property_mapping and property_mapping.get("confidence", 0) > 0.7:
            property_mapping["category"] = "properties"
            return property_mapping
        
        # Try mapping to an instance
        instance_mapping = self._map_to_instance(entity_text, query_context)
        if instance_mapping and instance_mapping.get("confidence", 0) > 0.7:
            instance_mapping["category"] = "instances"
            return instance_mapping
        return

    def _semantic_match(
        self, 
        text: str, 
        term_dict: Dict[str, Dict[str, Any]], 
        term_type: str
    ) -> List[Dict[str, Any]]:
        """
        Find semantic matches between text and ontology terms.
        
        Args:
            text: The text to match
            term_dict: Dictionary of terms to match against
            term_type: Type of terms ('class' or 'property')
            
        Returns:
            List of matches sorted by similarity
        """
        # Get embedding for the input text
        text_embedding = self.embedding_model.encode(text)
        
        matches = []
        
        for uri, term_info in term_dict.items():
            # Get labels and comments for semantic matching
            labels = term_info.get("labels", [])
            best_similarity = 0
            best_match_text = ""
            
            # Check similarity with each label
            for label in labels:
                # Get or compute label embedding
                label_key = f"{uri}_label_{label}"
                if label_key not in self.term_embeddings:
                    self.term_embeddings[label_key] = self.embedding_model.encode(label)
                
                similarity = self._cosine_similarity(text_embedding, self.term_embeddings[label_key])
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match_text = label
            
            # If the similarity is above threshold, add to matches
            if best_similarity > 0.5:
                matches.append({
                    "uri": uri,
                    "matched_label": best_match_text,
                    "similarity": best_similarity
                })
        
        # Sort matches by similarity
        matches.sort(key=lambda x: x["similarity"], reverse=True)
        
        # Return top matches
        return matches[:5]
    
    def _cosine_similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """Calculate cosine similarity between two embeddings."""
        return np.dot(embedding1, embedding2) / (np.linalg.norm(embedding1) * np.linalg.norm(embedding2))
    
    def _llm_based_mapping(
        self, 
        unknown_entities: List[Dict[str, Any]], 
        query_context: str,
        mapped_entities: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Use LLM to map entities that couldn't be mapped automatically.
        
        Args:
            unknown_entities: List of unmapped entities
            query_context: The full query for context
            mapped_entities: Already mapped entities for context
            
        Returns:
            Updated mapping with LLM suggestions
        """
        # Prepare ontology information for the LLM
        ontology_summary = self._prepare_ontology_summary()
        
        # Prepare mapped entities as context
        mapped_context = self._prepare_mapped_context(mapped_entities)
        
        # Prepare unknown entities as text
        unknown_text = "\n".join([f"- '{e['text']}' (detected type: {e['type']})" for e in unknown_entities])
        
        # Prepare prompt for the LLM
        prompt = f"""
I need you to help map natural language terms to formal ontology terms.

User Query:
"{query_context}"

Ontology Summary:
{ontology_summary}

Already Mapped Entities:
{mapped_context}

Unknown Entities to Map:
{unknown_text}

For each unknown entity:
1. Analyze the entity and the query context
2. Determine if it maps to a class, property, instance, or literal in the ontology
3. Provide the URI of the matching ontology term if possible
4. Assign a confidence score (0.0-1.0)

Return your mapping in the following JSON format:
```
{{
  "classes": [
    {{
      "text": "original_text",
      "uri": "mapped_uri",
      "label": "preferred_label",
      "confidence": 0.85
    }}
  ],
  "properties": [...],
  "instances": [...],
  "literals": [...],
  "unknown": [
    {{
      "text": "still_unmapped_text",
      "type": "UNKNOWN",
      "reason": "reason why mapping failed"
    }}
  ]
}}
```
        """
        # Get mapping from the LLM
        response = self.proxy.initiate_chat(self.agent, message=prompt)

        # Extract the mapping from the ChatResult response.
        response_text = response.summary.strip()
        
        # Parse the JSON result
        try:
            # Find JSON content in response
            import re
            json_match = re.search(r'```(json)?\s*({.*?})\s*```', response_text, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(2)
                mapping_result = json.loads(json_str)
            else:
                # Try to find JSON without the code block
                json_match = re.search(r'({.*})', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                    mapping_result = json.loads(json_str)
                else:
                    # Fallback if no JSON found
                    mapping_result = {
                        "classes": [],
                        "properties": [],
                        "instances": [],
                        "literals": [],
                        "unknown": unknown_entities
                    }
                    
            return mapping_result
        
        except Exception as e:
            print(f"Error parsing LLM mapping result: {e}")
            return {
                "classes": [],
                "properties": [],
                "instances": [],
                "literals": [],
                "unknown": unknown_entities
            }
    
    def _prepare_ontology_summary(self) -> str:
        """Prepare a summary of the ontology for the LLM."""
        # List key classes
        class_summary = "\nKey Classes:\n"
        class_count = 0
        for uri, info in self.class_hierarchy.items():
            if info["labels"]:
                label = info["labels"][0]
                class_summary += f"- {label} ({uri})\n"
                class_count += 1
                
                # Limit to avoid overwhelming the LLM
                if class_count >= 20:
                    class_summary += f"- ... and {len(self.class_hierarchy) - 20} more classes\n"
                    break
        
        # List key properties
        property_summary = "\nKey Properties:\n"
        property_count = 0
        for uri, info in self.property_domains_ranges.items():
            if info["labels"]:
                label = info["labels"][0]
                domains = ", ".join([d.split("/")[-1].split("#")[-1] for d in info["domains"][:2]]) if info["domains"] else "unspecified"
                ranges = ", ".join([r.split("/")[-1].split("#")[-1] for r in info["ranges"][:2]]) if info["ranges"] else "unspecified"
                
                property_summary += f"- {label} ({uri}) - Domain: {domains}, Range: {ranges}\n"
                property_count += 1
                
                # Limit to avoid overwhelming the LLM
                if property_count >= 20:
                    property_summary += f"- ... and {len(self.property_domains_ranges) - 20} more properties\n"
                    break
        
        return f"This ontology contains {len(self.class_hierarchy)} classes and {len(self.property_domains_ranges)} properties.{class_summary}{property_summary}"
    
    def _prepare_mapped_context(self, mapped_entities: Dict[str, List[Dict[str, Any]]]) -> str:
        """Prepare a summary of already mapped entities for the LLM."""
        context = ""
        
        # Add classes
        if mapped_entities["classes"]:
            context += "\nMapped Classes:\n"
            for entity in mapped_entities["classes"]:
                context += f"- '{entity['text']}' -> {entity['label']} ({entity['uri']})\n"
        
        # Add properties
        if mapped_entities["properties"]:
            context += "\nMapped Properties:\n"
            for entity in mapped_entities["properties"]:
                context += f"- '{entity['text']}' -> {entity['label']} ({entity['uri']})\n"
        
        # Add instances
        if mapped_entities["instances"]:
            context += "\nMapped Instances:\n"
            for entity in mapped_entities["instances"]:
                context += f"- '{entity['text']}' -> {entity['label']} ({entity['uri']})\n"
        
        # Add literals
        if mapped_entities["literals"]:
            context += "\nMapped Literals:\n"
            for entity in mapped_entities["literals"]:
                context += f"- '{entity['text']}' -> {entity['inferred_type']}\n"
        return context
