import json
import re
from typing import Any, Dict, List, Optional

import autogen

from config.agent_config import get_agent_config


class SPARQLValidationAgent:
    """
    Slave agent responsible for validating SPARQL queries.
    Checks syntax, semantics, and detects potential issues.
    """
    
    def __init__(self):
        """Initialize the SPARQL validation agent."""
        # Get configuration for agent
        agent_config = get_agent_config("sparql_validation")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction
        self.proxy = autogen.UserProxyAgent(
            name="SPARQLValidationProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
    
    def validate_query(
        self, 
        sparql_query: str, 
        query_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query to validate
            query_metadata: Metadata about the query
            
        Returns:
            Validation result with is_valid flag and feedback
        """
        # Basic syntax check
        syntax_result = self._check_syntax(sparql_query)
        
        if not syntax_result["is_valid"]:
            return syntax_result
        
        # Check semantic validity using LLM
        return self._llm_based_validation(sparql_query, query_metadata)
    
    def _check_syntax(self, sparql_query: str) -> Dict[str, Any]:
        """
        Perform basic syntax checks on SPARQL query.
        
        Args:
            sparql_query: The SPARQL query to check
            
        Returns:
            Validation result dictionary
        """
        # Check for empty query
        if not sparql_query or sparql_query.strip() == "":
            return {
                "is_valid": False,
                "validation_type": "syntax",
                "feedback": "Query is empty"
            }
        
        # Check for balanced braces
        if sparql_query.count("{") != sparql_query.count("}"):
            return {
                "is_valid": False,
                "validation_type": "syntax",
                "feedback": "Unbalanced braces in query"
            }
        
        # Check for proper query form (SELECT, ASK, CONSTRUCT, DESCRIBE)
        query_form_match = re.search(r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b', sparql_query, re.IGNORECASE)
        if not query_form_match:
            return {
                "is_valid": False,
                "validation_type": "syntax",
                "feedback": "Missing query form (SELECT, ASK, CONSTRUCT, DESCRIBE)"
            }
        
        # Check for WHERE clause (except for ASK queries where it's optional)
        if query_form_match.group(1).upper() != "ASK" and "WHERE" not in sparql_query.upper():
            return {
                "is_valid": False,
                "validation_type": "syntax",
                "feedback": "Missing WHERE clause"
            }
        
        # Check for proper PREFIX definitions
        prefix_matches = re.findall(r'PREFIX\s+([^:]+):\s*<([^>]+)>', sparql_query, re.IGNORECASE)
        
        for prefix, uri in prefix_matches:
            # Check if prefix is valid
            if not prefix.strip():
                return {
                    "is_valid": False,
                    "validation_type": "syntax",
                    "feedback": f"Invalid empty prefix in: PREFIX {prefix}: <{uri}>"
                }
            
            # Check if URI is valid
            if not uri or not re.match(r'^https?://', uri):
                return {
                    "is_valid": False,
                    "validation_type": "syntax",
                    "feedback": f"Invalid URI in: PREFIX {prefix}: <{uri}>"
                }
        
        # Check if all triple patterns have subject, predicate, and object
        # This is a basic check; a full parser would be more accurate
        where_section_match = re.search(r'WHERE\s*{([^}]+)}', sparql_query, re.IGNORECASE | re.DOTALL)
        
        if where_section_match:
            where_section = where_section_match.group(1)
            
            # Remove FILTER, OPTIONAL, and other complex patterns for simple validation
            simplified_section = re.sub(r'FILTER\s*\([^)]+\)', '', where_section, flags=re.IGNORECASE)
            simplified_section = re.sub(r'OPTIONAL\s*{[^}]+}', '', simplified_section, flags=re.IGNORECASE)
            
            # Split into triples (simple approach, not perfect)
            triple_patterns = [t.strip() for t in simplified_section.split('.') if t.strip()]
            
            for pattern in triple_patterns:
                parts = [p.strip() for p in pattern.split() if p.strip()]
                
                # Expect at least subject, predicate, object
                if len(parts) < 3:
                    return {
                        "is_valid": False,
                        "validation_type": "syntax",
                        "feedback": f"Incomplete triple pattern: {pattern}"
                    }
        
        # If we get here, the basic syntax checks pass
        return {
            "is_valid": True,
            "validation_type": "syntax",
            "feedback": "Basic syntax check passed"
        }
    
    def _llm_based_validation(self, sparql_query: str, query_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to validate SPARQL query semantics.
        
        Args:
            sparql_query: The SPARQL query to validate
            query_metadata: Metadata about the query
            
        Returns:
            Validation result dictionary
        """
        # Format the entities used
        entities_str = ""
        entities_used = query_metadata.get("entities_used", {})
        
        if entities_used.get("classes", []):
            entities_str += "\nClasses:\n"
            for entity in entities_used["classes"]:
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']})\n"
        
        if entities_used.get("properties", []):
            entities_str += "\nProperties:\n"
            for entity in entities_used["properties"]:
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']})\n"
        
        if entities_used.get("instances", []):
            entities_str += "\nInstances:\n"
            for entity in entities_used["instances"]:
                entities_str += f"- {entity.get('label', entity['text'])} ({entity['uri']})\n"
        
        # Prepare the prompt for the LLM
        prompt = f"""
I need you to validate a SPARQL query for semantic correctness and potential issues.

SPARQL Query:
```sparql
{sparql_query}
```

Query Type: {query_metadata.get("query_type", "Unknown")}
Template-based: {query_metadata.get("template_based", False)}

Entities Used in Query:
{entities_str}

Please validate the query for:
1. Semantic correctness (do the triple patterns make logical sense?)
2. Proper use of variables (are variables used consistently?)
3. Proper use of entities (are URIs used appropriately?)
4. Performance considerations (are there potential efficiency issues?)
5. Any other potential problems

Return your validation result in the following JSON format:
```
{{
  "is_valid": true/false,
  "validation_type": "semantic",
  "feedback": "detailed feedback on issues found or confirmation of validity",
  "suggestions": ["improvement suggestion 1", "improvement suggestion 2"] // Optional
}}
```
"""
        
        # Get validation result from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        
        # Extract the result from the response
        response_text = response.get("content", "").strip()
        
        # Parse the JSON result
        try:
            # Find JSON content in response
            import re
            json_match = re.search(r'```(?:json)?\s*({.+?})\s*```', response_text, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
                validation_result = json.loads(json_str)
            else:
                # Try to find JSON without the code block
                json_match = re.search(r'({.+})', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                    validation_result = json.loads(json_str)
                else:
                    # Fallback if no JSON found
                    validation_result = {
                        "is_valid": False,
                        "validation_type": "semantic",
                        "feedback": "Unable to validate query due to parsing error"
                    }
        except Exception as e:
            print(f"Error parsing validation result: {e}")
            validation_result = {
                "is_valid": False,
                "validation_type": "semantic",
                "feedback": f"Error validating query: {str(e)}"
            }
        
        return validation_result
