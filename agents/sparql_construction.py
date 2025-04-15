import json
import os
from typing import Any, Dict, List, Optional

import autogen

from config.agent_config import get_agent_config


class SPARQLConstructionAgent:
    """
    Slave agent responsible for constructing SPARQL queries.
    Builds queries based on intent, mapped entities, and query patterns.
    """
    
    def __init__(self, templates_dir: Optional[str] = None):
        """
        Initialize the SPARQL construction agent.
        
        Args:
            templates_dir: Directory containing SPARQL query templates
        """
        # Get configuration for agent
        agent_config = get_agent_config("sparql_construction")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction
        self.proxy = autogen.UserProxyAgent(
            name="SPARQLConstructionProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
        
        # Load SPARQL templates
        self.templates_dir = templates_dir or os.path.join(os.path.dirname(__file__), "../templates/sparql")
        self.templates = self._load_templates()
        
        # Common prefixes for SPARQL queries
        self.common_prefixes = {
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        }
    
    def construct_query(
        self, 
        refined_query: str,
        mapped_entities: Dict[str, List[Dict[str, Any]]],
        query_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Construct a SPARQL query based on the user query and mapped entities.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of entities mapped to ontology terms
            query_type: Optional explicit query type (SELECT, ASK, CONSTRUCT, DESCRIBE)
            
        Returns:
            Dictionary containing the SPARQL query and metadata
        """
        # Determine query type if not provided
        if not query_type:
            query_type = self._determine_query_type(refined_query, mapped_entities)
        
        # Try to find a suitable template
        template = self._find_template(refined_query, mapped_entities, query_type)
        
        # If a template is found, try to fill it
        if template:
            try:
                query = self._fill_template(template, mapped_entities)
                
                # Add prefixes
                prefixed_query = self._add_prefixes(query, mapped_entities)
                
                return {
                    "sparql": prefixed_query,
                    "query_type": query_type,
                    "template_id": template.get("id"),
                    "template_based": True,
                    "entities_used": self._get_entities_used(mapped_entities)
                }
            except Exception as e:
                print(f"Error filling template: {e}")
                # Fall back to LLM-based construction
        
        # If no template found or template filling failed, use LLM
        return self._llm_based_construction(refined_query, mapped_entities, query_type)
    
    def _load_templates(self) -> List[Dict[str, Any]]:
        """Load SPARQL query templates from the templates directory."""
        templates = []
        
        # Create templates directory if it doesn't exist
        if not os.path.exists(self.templates_dir):
            os.makedirs(self.templates_dir)
            self._create_example_templates()
        
        # Load all JSON files in the templates directory
        try:
            for filename in os.listdir(self.templates_dir):
                if filename.endswith(".json"):
                    with open(os.path.join(self.templates_dir, filename), "r") as f:
                        template_data = json.load(f)
                        
                        # Add the template if it has the required fields
                        if "id" in template_data and "pattern" in template_data:
                            templates.append(template_data)
        except Exception as e:
            print(f"Error loading templates: {e}")
        
        print(f"Loaded {len(templates)} SPARQL query templates")
        return templates
    
    def _create_example_templates(self):
        """Create example SPARQL query templates."""
        example_templates = [
            {
                "id": "class_instances",
                "name": "Get all instances of a class",
                "description": "Returns all instances of a specific class",
                "query_type": "SELECT",
                "keywords": ["what", "instances", "list", "all", "show"],
                "requires": {
                    "classes": 1,
                    "properties": 0,
                    "instances": 0
                },
                "pattern": """
                SELECT ?instance ?label
                WHERE {
                    ?instance a <{class_uri}> .
                    OPTIONAL { ?instance rdfs:label ?label . }
                }
                """
            },
            {
                "id": "instance_properties",
                "name": "Get all properties of an instance",
                "description": "Returns all properties and values for a specific instance",
                "query_type": "SELECT",
                "keywords": ["properties", "attributes", "information", "details", "about"],
                "requires": {
                    "classes": 0,
                    "properties": 0,
                    "instances": 1
                },
                "pattern": """
                SELECT ?property ?value
                WHERE {
                    <{instance_uri}> ?property ?value .
                }
                """
            },
            {
                "id": "property_values",
                "name": "Get specific property values",
                "description": "Returns values of a specific property for instances of a class",
                "query_type": "SELECT",
                "keywords": ["what", "value", "who", "has", "with"],
                "requires": {
                    "classes": 1,
                    "properties": 1,
                    "instances": 0
                },
                "pattern": """
                SELECT ?instance ?value
                WHERE {
                    ?instance a <{class_uri}> .
                    ?instance <{property_uri}> ?value .
                    OPTIONAL { ?instance rdfs:label ?label . }
                }
                """
            },
            {
                "id": "instance_exists",
                "name": "Check if an instance exists",
                "description": "Checks if a specific instance exists with certain properties",
                "query_type": "ASK",
                "keywords": ["exists", "is there", "does", "has", "check"],
                "requires": {
                    "classes": 0,
                    "properties": 1,
                    "instances": 1
                },
                "pattern": """
                ASK {
                    <{instance_uri}> <{property_uri}> ?value .
                }
                """
            },
            {
                "id": "filtered_instances",
                "name": "Get filtered instances",
                "description": "Returns instances of a class with property values matching a filter",
                "query_type": "SELECT",
                "keywords": ["where", "filter", "greater", "less", "equal", "contains"],
                "requires": {
                    "classes": 1,
                    "properties": 1,
                    "literals": 1
                },
                "pattern": """
                SELECT ?instance ?label
                WHERE {
                    ?instance a <{class_uri}> .
                    ?instance <{property_uri}> ?value .
                    OPTIONAL { ?instance rdfs:label ?label . }
                    FILTER ({filter_condition})
                }
                """
            }
        ]
        
        # Save example templates
        for template in example_templates:
            template_path = os.path.join(self.templates_dir, f"{template['id']}.json")
            with open(template_path, "w") as f:
                json.dump(template, f, indent=2)
        
        print(f"Created {len(example_templates)} example templates")
    
    def _determine_query_type(
        self, 
        query: str, 
        mapped_entities: Dict[str, List[Dict[str, Any]]]
    ) -> str:
        """
        Determine the appropriate SPARQL query type.
        
        Args:
            query: The refined user query
            mapped_entities: Dictionary of mapped entities
            
        Returns:
            SPARQL query type (SELECT, ASK, CONSTRUCT, DESCRIBE)
        """
        query_lower = query.lower()
        
        # ASK queries check if something exists
        if any(keyword in query_lower for keyword in [
            "is there", "does", "do", "exists", "has", "is it", "can", "check if"
        ]):
            return "ASK"
        
        # DESCRIBE queries request all information about a resource
        if any(keyword in query_lower for keyword in [
            "describe", "tell me about", "information about", "details about", "description of"
        ]) and (mapped_entities["instances"] or mapped_entities["classes"]):
            return "DESCRIBE"
        
        # CONSTRUCT queries create new triples
        if any(keyword in query_lower for keyword in [
            "create", "construct", "build", "generate graph", "make graph"
        ]):
            return "CONSTRUCT"
        
        # Default to SELECT for most queries
        return "SELECT"
    
    def _find_template(
        self, 
        query: str, 
        mapped_entities: Dict[str, List[Dict[str, Any]]],
        query_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Find the most suitable template for the query.
        
        Args:
            query: The refined user query
            mapped_entities: Dictionary of mapped entities
            query_type: The determined query type
            
        Returns:
            The best matching template or None if no suitable template found
        """
        # Filter templates by query type
        candidates = [t for t in self.templates if t.get("query_type") == query_type]
        
        if not candidates:
            return None
        
        # Filter by required entity types
        valid_candidates = []
        for template in candidates:
            requirements = template.get("requires", {})
            
            # Check if the template requirements are met
            meets_requirements = True
            for entity_type, count in requirements.items():
                if len(mapped_entities.get(entity_type, [])) < count:
                    meets_requirements = False
                    break
            
            if meets_requirements:
                valid_candidates.append(template)
        
        if not valid_candidates:
            return None
        
        # Score remaining templates by keyword matching
        scored_candidates = []
        query_lower = query.lower()
        
        for template in valid_candidates:
            keywords = template.get("keywords", [])
            score = sum(1 for keyword in keywords if keyword in query_lower)
            scored_candidates.append((template, score))
        
        # Sort by score
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        
        # Return the highest scoring template if it has a positive score
        if scored_candidates and scored_candidates[0][1] > 0:
            return scored_candidates[0][0]
        
        # If no good match by keywords, return the first valid template
        if valid_candidates:
            return valid_candidates[0]
        
        return None
    
    def _fill_template(
        self, 
        template: Dict[str, Any], 
        mapped_entities: Dict[str, List[Dict[str, Any]]]
    ) -> str:
        """
        Fill template with entity values.
        
        Args:
            template: The template to fill
            mapped_entities: Dictionary of mapped entities
            
        Returns:
            Filled SPARQL query string
        """
        pattern = template["pattern"]
        
        # Extract required entity types from template
        requirements = template.get("requires", {})
        
        # Create a mapping of placeholders to entity values
        replacements = {}
        
        # Handle class URIs
        if "classes" in requirements and requirements["classes"] > 0:
            for i in range(requirements["classes"]):
                if i < len(mapped_entities["classes"]):
                    placeholder = f"{{class_uri}}" if i == 0 else f"{{class_{i+1}_uri}}"
                    replacements[placeholder] = mapped_entities["classes"][i]["uri"]
        
        # Handle property URIs
        if "properties" in requirements and requirements["properties"] > 0:
            for i in range(requirements["properties"]):
                if i < len(mapped_entities["properties"]):
                    placeholder = f"{{property_uri}}" if i == 0 else f"{{property_{i+1}_uri}}"
                    replacements[placeholder] = mapped_entities["properties"][i]["uri"]
        
        # Handle instance URIs
        if "instances" in requirements and requirements["instances"] > 0:
            for i in range(requirements["instances"]):
                if i < len(mapped_entities["instances"]):
                    placeholder = f"{{instance_uri}}" if i == 0 else f"{{instance_{i+1}_uri}}"
                    replacements[placeholder] = mapped_entities["instances"][i]["uri"]
        
        # Handle literals and filters
        if "literals" in requirements and requirements["literals"] > 0:
            # Handle literals differently based on their inferred types
            for i in range(requirements["literals"]):
                if i < len(mapped_entities["literals"]):
                    literal = mapped_entities["literals"][i]
                    literal_text = literal["text"]
                    literal_type = literal.get("inferred_type", "xsd:string")
                    
                    # Format based on type
                    if literal_type == "xsd:string":
                        formatted_value = f'"{literal_text}"'
                    elif literal_type in ["xsd:integer", "xsd:decimal", "xsd:float", "xsd:double"]:
                        formatted_value = literal_text
                    elif literal_type in ["xsd:date", "xsd:dateTime"]:
                        formatted_value = f'"{literal_text}"^^{literal_type}'
                    else:
                        formatted_value = f'"{literal_text}"'
                    
                    placeholder = f"{{literal_value}}" if i == 0 else f"{{literal_{i+1}_value}}"
                    replacements[placeholder] = formatted_value
            
            # Handle filter condition if needed
            if "{filter_condition}" in pattern:
                if len(mapped_entities["properties"]) > 0 and len(mapped_entities["literals"]) > 0:
                    prop = mapped_entities["properties"][0]
                    literal = mapped_entities["literals"][0]
                    
                    # Determine appropriate operator based on the query and property type
                    operator = "="  # Default operator
                    
                    # Try to infer operator from property range
                    if "ranges" in prop:
                        range_type = prop["ranges"][0] if prop["ranges"] else ""
                        if any(num_type in range_type for num_type in ["Integer", "Decimal", "Float", "Double"]):
                            # Numeric comparison
                            if "greater" in template.get("id", "").lower():
                                operator = ">"
                            elif "less" in template.get("id", "").lower():
                                operator = "<"
                    
                    # Format the filter condition
                    filter_condition = f"?value {operator} {replacements.get('{{literal_value}}', literal['text'])}"
                    replacements["{filter_condition}"] = filter_condition
        
        # Replace all placeholders
        filled_query = pattern
        for placeholder, value in replacements.items():
            filled_query = filled_query.replace(placeholder, value)
        
        return filled_query.strip()
    
    def _add_prefixes(self, query: str, mapped_entities: Dict[str, List[Dict[str, Any]]]) -> str:
        """
        Add appropriate prefixes to the SPARQL query.
        
        Args:
            query: The SPARQL query string
            mapped_entities: Dictionary of mapped entities
            
        Returns:
            Query with prefixes
        """
        # Collect all URIs from mapped entities
        all_uris = []
        for entity_type in ["classes", "properties", "instances"]:
            for entity in mapped_entities.get(entity_type, []):
                if "uri" in entity:
                    all_uris.append(entity["uri"])
        
        # Determine which prefixes are needed
        needed_prefixes = {}
        
        # Always include common prefixes
        needed_prefixes.update(self.common_prefixes)
        
        # Add ontology-specific prefixes based on URIs
        for uri in all_uris:
            # Extract namespace from URI
            if "#" in uri:
                namespace = uri.split("#")[0] + "#"
            elif "/" in uri:
                namespace = uri.rsplit("/", 1)[0] + "/"
            else:
                continue
            
            # Find an appropriate prefix
            # For simplicity, use the last part of the namespace
            if namespace not in needed_prefixes.values():
                parts = namespace.rstrip("#/").split("/")
                prefix = parts[-1].lower()
                
                # Avoid duplicate prefixes
                if prefix in needed_prefixes:
                    prefix = f"{prefix}{len(needed_prefixes)}"
                
                needed_prefixes[prefix] = namespace
        
        # Format prefix declarations
        prefix_str = ""
        for prefix, uri in needed_prefixes.items():
            prefix_str += f"PREFIX {prefix}: <{uri}>\n"
        
        return prefix_str + "\n" + query
    
    def _get_entities_used(self, mapped_entities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract the subset of entities that were used in the query.
        
        Args:
            mapped_entities: Dictionary of all mapped entities
            
        Returns:
            Dictionary of entities used in the query
        """
        # For now, just return all entities
        # In a more sophisticated implementation, this would track which entities were actually used
        return mapped_entities
    
    def _llm_based_construction(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, List[Dict[str, Any]]],
        query_type: str
    ) -> Dict[str, Any]:
        """
        Use LLM to construct a SPARQL query when templates don't match.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped entities
            query_type: The determined query type
            
        Returns:
            Dictionary containing the LLM-generated SPARQL query and metadata
        """
        # Format the mapped entities
        entities_str = ""
        
        if mapped_entities["classes"]:
            entities_str += "\nClasses:\n"
            for entity in mapped_entities["classes"]:
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']})\n"
        
        if mapped_entities["properties"]:
            entities_str += "\nProperties:\n"
            for entity in mapped_entities["properties"]:
                domains = ", ".join(entity.get("domains", [])) if "domains" in entity else "unspecified"
                ranges = ", ".join(entity.get("ranges", [])) if "ranges" in entity else "unspecified"
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']}) - Domain: {domains}, Range: {ranges}\n"
        
        if mapped_entities["instances"]:
            entities_str += "\nInstances:\n"
            for entity in mapped_entities["instances"]:
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']}) - Type: {entity.get('instance_type', 'unspecified')}\n"
        
        if mapped_entities["literals"]:
            entities_str += "\nLiterals:\n"
            for entity in mapped_entities["literals"]:
                entities_str += f"- {entity['text']} (Type: {entity.get('inferred_type', 'unspecified')})\n"
        
        # Format common prefixes
        prefixes_str = "\nCommon prefixes:\n"
        for prefix, uri in self.common_prefixes.items():
            prefixes_str += f"PREFIX {prefix}: <{uri}>\n"
        
        # Prepare the prompt for the LLM
        prompt = f"""
I need you to construct a valid SPARQL query based on a natural language question.

User Question:
"{refined_query}"

SPARQL Query Type:
{query_type}

Mapped Ontology Entities:
{entities_str}

{prefixes_str}

Please create a SPARQL query that:
1. Uses the correct query form ({query_type})
2. Incorporates the mapped entities where appropriate
3. Uses proper SPARQL syntax and conventions
4. Adds appropriate variable names (?instance, ?label, etc.)
5. Includes any necessary additional patterns, FILTER, OPTIONAL, etc.
6. Includes appropriate prefixes for all URIs

Return only the complete SPARQL query with prefixes.
"""
        
        # Get SPARQL query from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        response_text = response.summary.strip()
        
        # Extract the SPARQL query (ignore explanation text)
        import re
        sparql_pattern = re.compile(r'(?:```(?:sparql)?\s*)?(.+?)(?:\s*```)?$', re.DOTALL)
        match = sparql_pattern.search(response_text)
        
        if match:
            sparql_query = match.group(1).strip()
        else:
            sparql_query = response_text
        
        # Return the query and metadata
        return {
            "sparql": sparql_query,
            "query_type": query_type,
            "template_based": False,
            "llm_generated": True,
            "entities_used": mapped_entities
        }
