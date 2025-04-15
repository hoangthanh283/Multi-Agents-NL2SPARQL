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
