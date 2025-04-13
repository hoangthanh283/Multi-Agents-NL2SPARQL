"""
SPARQL Tools module for the NL to SPARQL system.
Provides utility functions for working with SPARQL queries.
"""

import re
from typing import Any, Dict, List, Optional


class SPARQLTools:
    """Utility class for working with SPARQL queries."""
    
    @staticmethod
    def add_prefixes(sparql_query: str, prefixes: Dict[str, str]) -> str:
        """
        Add namespace prefixes to a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query
            prefixes: Dictionary of prefix names to URIs
            
        Returns:
            Query with prefixes added
        """
        # Check if query already has PREFIX declarations
        if re.search(r'PREFIX\s+', sparql_query, re.IGNORECASE):
            return sparql_query
            
        # Format PREFIX declarations
        prefix_str = ""
        for prefix, uri in prefixes.items():
            prefix_str += f"PREFIX {prefix}: <{uri}>\n"
            
        return prefix_str + "\n" + sparql_query
    
    @staticmethod
    def format_term(term: str, term_type: str) -> str:
        """
        Format a term (URI, literal, variable) for use in a SPARQL query.
        
        Args:
            term: The term to format
            term_type: The type of term (uri, literal, var)
            
        Returns:
            Formatted term
        """
        if term_type == "uri":
            # Check if already formatted as URI
            if term.startswith("<") and term.endswith(">"):
                return term
            return f"<{term}>"
            
        elif term_type == "literal":
            # Simple string literal
            if not term.startswith('"') and not term.startswith("'"):
                return f'"{term}"'
            return term
            
        elif term_type == "var":
            # Variable
            if not term.startswith("?"):
                return f"?{term}"
            return term
            
        # Default return the term unchanged
        return term
    
    @staticmethod
    def format_literal(value: Any, datatype: Optional[str] = None) -> str:
        """
        Format a literal value for use in a SPARQL query.
        
        Args:
            value: The literal value
            datatype: Optional XSD datatype
            
        Returns:
            Formatted literal
        """
        # String literal
        if isinstance(value, str):
            if datatype:
                return f'"{value}"^^{datatype}'
            return f'"{value}"'
            
        # Numeric literal
        elif isinstance(value, (int, float)):
            if datatype:
                return f'"{value}"^^{datatype}'
            return str(value)
            
        # Boolean literal
        elif isinstance(value, bool):
            if datatype == "xsd:boolean":
                return f'"{str(value).lower()}"^^xsd:boolean'
            return str(value).lower()
            
        # Default
        else:
            if datatype:
                return f'"{str(value)}"^^{datatype}'
            return f'"{str(value)}"'
    
    @staticmethod
    def extract_query_type(sparql_query: str) -> str:
        """
        Extract the query type (SELECT, ASK, CONSTRUCT, DESCRIBE) from a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query
            
        Returns:
            Query type
        """
        # Remove comments
        query_without_comments = re.sub(r'#.*$', '', sparql_query, flags=re.MULTILINE)
        
        # Look for query form
        match = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', query_without_comments, re.IGNORECASE)
        
        if match:
            return match.group(1).upper()
        
        return "UNKNOWN"
    
    @staticmethod
    def extract_variables(sparql_query: str) -> List[str]:
        """
        Extract the variables from a SELECT query.
        
        Args:
            sparql_query: The SPARQL query
            
        Returns:
            List of variable names (without ?)
        """
        # Check if it's a SELECT query
        if not re.search(r'\bSELECT\b', sparql_query, re.IGNORECASE):
            return []
            
        # Extract SELECT clause
        match = re.search(r'\bSELECT\b\s+(.+?)\s*\bWHERE\b', sparql_query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return []
            
        select_clause = match.group(1)
        
        # Handle SELECT * case
        if '*' in select_clause:
            # Extract variables from WHERE clause
            where_clause = re.search(r'\bWHERE\b\s*{(.+)}', sparql_query, re.IGNORECASE | re.DOTALL)
            if where_clause:
                # Find all variables in triple patterns
                variables = re.findall(r'\?([a-zA-Z0-9_]+)', where_clause.group(1))
                return list(set(variables))  # Remove duplicates
            return []
        
        # Extract named variables
        variables = re.findall(r'\?([a-zA-Z0-9_]+)', select_clause)
        return list(set(variables))  # Remove duplicates
    
    @staticmethod
    def simplify_results(sparql_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simplify SPARQL results for easier consumption.
        
        Args:
            sparql_results: Raw SPARQL results
            
        Returns:
            Simplified results
        """
        simplified = []
        
        # Handle boolean results (ASK queries)
        if "boolean" in sparql_results:
            return [{"result": sparql_results["boolean"]}]
        
        # Handle bindings results (SELECT queries)
        if "results" in sparql_results and "bindings" in sparql_results["results"]:
            for binding in sparql_results["results"]["bindings"]:
                simple_binding = {}
                
                for var, value in binding.items():
                    # Extract the value and add type info
                    if value["type"] == "uri":
                        simple_binding[var] = {
                            "value": value["value"],
                            "type": "uri"
                        }
                    elif value["type"] == "literal":
                        simple_binding[var] = {
                            "value": value["value"],
                            "type": "literal"
                        }
                        # Add datatype if present
                        if "datatype" in value:
                            simple_binding[var]["datatype"] = value["datatype"]
                        # Add language tag if present
                        if "xml:lang" in value:
                            simple_binding[var]["language"] = value["xml:lang"]
                    else:
                        simple_binding[var] = {
                            "value": value["value"],
                            "type": value["type"]
                        }
                
                simplified.append(simple_binding)
        
        return simplified


# tools/template_tools.py
"""
Template Tools module for the NL to SPARQL system.
Provides utility functions for working with SPARQL query templates.
"""

import json
import os
import re
from typing import Any, Dict, List, Optional


class TemplateTools:
    """Utility class for working with SPARQL query templates."""
    
    def __init__(self, templates_dir: str = None):
        """
        Initialize the template tools.
        
        Args:
            templates_dir: Directory containing SPARQL query templates
        """
        self.templates_dir = templates_dir or os.path.join(os.path.dirname(__file__), "../templates/sparql")
        self.templates = self._load_templates()
    
    def _load_templates(self) -> List[Dict[str, Any]]:
        """
        Load SPARQL query templates from the templates directory.
        
        Returns:
            List of template dictionaries
        """
        templates = []
        
        # Create templates directory if it doesn't exist
        if not os.path.exists(self.templates_dir):
            os.makedirs(self.templates_dir)
        
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
        
        return templates
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a template by ID.
        
        Args:
            template_id: ID of the template
            
        Returns:
            Template dictionary or None if not found
        """
        for template in self.templates:
            if template.get("id") == template_id:
                return template
        return None
    
    def find_templates_by_query_type(self, query_type: str) -> List[Dict[str, Any]]:
        """
        Find templates by query type.
        
        Args:
            query_type: SPARQL query type (SELECT, ASK, CONSTRUCT, DESCRIBE)
            
        Returns:
            List of matching templates
        """
        return [t for t in self.templates if t.get("query_type") == query_type.upper()]
    
    def find_templates_by_keywords(self, keywords: List[str]) -> List[Dict[str, Any]]:
        """
        Find templates matching keywords.
        
        Args:
            keywords: List of keywords to match
            
        Returns:
            List of matching templates with scores
        """
        scored_templates = []
        
        for template in self.templates:
            template_keywords = template.get("keywords", [])
            
            # Calculate score based on keyword matches
            score = sum(1 for k in keywords if k.lower() in template_keywords)
            
            if score > 0:
                scored_templates.append({
                    "template": template,
                    "score": score
                })
        
        # Sort by score
        scored_templates.sort(key=lambda x: x["score"], reverse=True)
        
        return scored_templates
    
    def find_templates_for_entities(
        self, 
        entity_counts: Dict[str, int]
    ) -> List[Dict[str, Any]]:
        """
        Find templates that match the given entity counts.
        
        Args:
            entity_counts: Dictionary of entity types and counts
            
        Returns:
            List of matching templates
        """
        matching_templates = []
        
        for template in self.templates:
            requirements = template.get("requires", {})
            
            # Check if template requirements are met
            matches = True
            for entity_type, required_count in requirements.items():
                if entity_counts.get(entity_type, 0) < required_count:
                    matches = False
                    break
            
            if matches:
                matching_templates.append(template)
        
        return matching_templates
    
    def fill_template(
        self, 
        template: Dict[str, Any], 
        entity_values: Dict[str, Dict[str, Any]]
    ) -> str:
        """
        Fill a template with entity values.
        
        Args:
            template: Template dictionary
            entity_values: Dictionary of entity values by type
            
        Returns:
            Filled SPARQL query
        """
        pattern = template["pattern"]
        
        # Create a mapping of placeholders to values
        replacements = {}
        
        # Handle class URIs
        for i, cls in enumerate(entity_values.get("classes", [])):
            if "uri" in cls:
                placeholder = f"{{class_uri}}" if i == 0 else f"{{class_{i+1}_uri}}"
                replacements[placeholder] = cls["uri"]
        
        # Handle property URIs
        for i, prop in enumerate(entity_values.get("properties", [])):
            if "uri" in prop:
                placeholder = f"{{property_uri}}" if i == 0 else f"{{property_{i+1}_uri}}"
                replacements[placeholder] = prop["uri"]
        
        # Handle instance URIs
        for i, inst in enumerate(entity_values.get("instances", [])):
            if "uri" in inst:
                placeholder = f"{{instance_uri}}" if i == 0 else f"{{instance_{i+1}_uri}}"
                replacements[placeholder] = inst["uri"]
        
        # Handle literals
        for i, lit in enumerate(entity_values.get("literals", [])):
            if "value" in lit:
                placeholder = f"{{literal_value}}" if i == 0 else f"{{literal_{i+1}_value}}"
                
                # Format based on datatype
                datatype = lit.get("datatype", "xsd:string")
                value = lit["value"]
                
                if datatype == "xsd:string":
                    replacements[placeholder] = f'"{value}"'
                elif datatype in ["xsd:integer", "xsd:decimal", "xsd:float", "xsd:double"]:
                    replacements[placeholder] = value
                elif datatype in ["xsd:date", "xsd:dateTime"]:
                    replacements[placeholder] = f'"{value}"^^{datatype}'
                else:
                    replacements[placeholder] = f'"{value}"'
        
        # Handle filter condition
        if "{filter_condition}" in pattern:
            filter_condition = self._build_filter_condition(entity_values)
            if filter_condition:
                replacements["{filter_condition}"] = filter_condition
        
        # Replace all placeholders
        filled_query = pattern
        for placeholder, value in replacements.items():
            filled_query = filled_query.replace(placeholder, value)
        
        return filled_query
    
    def _build_filter_condition(self, entity_values: Dict[str, Dict[str, Any]]) -> str:
        """
        Build a FILTER condition for a query.
        
        Args:
            entity_values: Dictionary of entity values by type
            
        Returns:
            FILTER condition string
        """
        if not entity_values.get("literals") or not entity_values.get("properties"):
            return ""
        
        # Get the first property and literal
        prop = entity_values["properties"][0]
        lit = entity_values["literals"][0]
        
        # Determine appropriate operator
        operator = "="  # Default
        
        # Try to infer operator based on property range and literal type
        if "ranges" in prop:
            range_type = prop["ranges"][0] if prop.get("ranges") else ""
            datatype = lit.get("datatype", "xsd:string")
            
            if any(num_type in range_type for num_type in ["Integer", "Decimal", "Float", "Double"]):
                # For numeric comparisons, look at context in literal
                context = lit.get("context", "").lower()
                if "greater" in context or "more" in context or "above" in context:
                    operator = ">"
                elif "less" in context or "fewer" in context or "below" in context:
                    operator = "<"
                elif "equal" in context or "exactly" in context:
                    operator = "="
        
        # Format the value based on datatype
        value = lit["value"]
        datatype = lit.get("datatype", "xsd:string")
        
        if datatype == "xsd:string":
            formatted_value = f'"{value}"'
        elif datatype in ["xsd:integer", "xsd:decimal", "xsd:float", "xsd:double"]:
            formatted_value = value
        elif datatype in ["xsd:date", "xsd:dateTime"]:
            formatted_value = f'"{value}"^^{datatype}'
        else:
            formatted_value = f'"{value}"'
        
        # Build the condition
        return f"?value {operator} {formatted_value}"
