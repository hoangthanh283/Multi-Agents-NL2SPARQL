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
