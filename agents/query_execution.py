import time
import os
from typing import Any, Dict, Optional

from SPARQLWrapper import (CSV, JSON, N3, RDFXML, TSV, TURTLE, XML,
                           SPARQLWrapper)

# Configure logging
from utils.logging_utils import setup_logging
from caches.query_cache import SPARQLQueryCache

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryExecutionAgent:
    """
    Slave agent responsible for executing SPARQL queries.
    Handles communication with SPARQL endpoints, authentication, and result formatting.
    """
    
    def __init__(
        self, 
        endpoint_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        default_graph: Optional[str] = None,
        redis_host: str = os.getenv("REDIS_HOST", "localhost"),
        redis_port: str = os.getenv("REDIS_PORT", "6379"),
        redis_ttl: str = os.getenv("REDIS_TTL", "3600"),
    ):
        """
        Initialize the query execution agent.
        
        Args:
            endpoint_url: URL of the SPARQL endpoint
            auth_token: Authentication token for the endpoint
            default_graph: Default graph URI
        """
        # Initialize endpoint settings
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.default_graph = default_graph

        # Result cache
        if not(redis_host) or not(redis_port) or not(redis_ttl):
            self.result_cache = {}
        else:
            self.result_cache = SPARQLQueryCache(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_ttl=redis_ttl
            )
        
        # # Result cache
        # self.result_cache = {}
        
        # Default timeout in seconds
        self.timeout = 30
        
        # Default result format
        self.result_format = JSON
        self.query_prefix = "cache:sparql:"
        
        # Map of format strings to SPARQLWrapper constants
        self.format_map = {
            "json": JSON,
            "xml": XML,
            "rdf": RDFXML,
            "n3": N3,
            "turtle": TURTLE,
            "csv": CSV,
            "tsv": TSV
        }
    
    def execute_query(
        self, 
        sparql_query: str, 
        endpoint_url: Optional[str] = None,
        result_format: str = "json",
        use_cache: bool = True,
        user_query: Optional[str] = None,
        context: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Execute a SPARQL query against an endpoint.
        
        Args:
            sparql_query: The SPARQL query to execute
            endpoint_url: Optional URL to override the default endpoint
            result_format: Format for the results (json, xml, etc.)
            use_cache: Whether to use cached results if available
            user_query: The natural language query provided by the user
            context: Additional context for the query
            
        Returns:
            Query execution results
        """
        # Use provided endpoint or default
        endpoint = endpoint_url or self.endpoint_url
        if not endpoint:
            return {
                "success": False,
                "error": "No SPARQL endpoint specified"
            }
        format_const = self.format_map.get(result_format.lower(), JSON)

        cache_entry = None
        if use_cache:
            cache_entry = self.result_cache.search(sparql_query, self.query_prefix)
            if cache_entry:
                return cache_entry
            
        try:
            # Initialize SPARQL wrapper
            sparql = SPARQLWrapper(endpoint)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(format_const)
            sparql.setTimeout(self.timeout)

            # Set default graph if specified
            if self.default_graph:
                sparql.addDefaultGraph(self.default_graph)

            # Set authentication if available
            if self.auth_token:
                sparql.addCustomHttpHeader("Authorization", f"Bearer {self.auth_token}")
                
            logger.info(f"Executing SPARQL query: {sparql_query[:50]}...")
            start_time = time.time()
            results = sparql.query()
            execution_time = time.time() - start_time
            
            if format_const == JSON:
                result_data = results.convert()
                formatted_result = self._format_json_results(result_data, sparql_query)
            elif format_const in [XML, RDFXML]:
                result_data = results.convert()
                formatted_result = {
                    "format": "xml",
                    "data": str(result_data),
                    "info": "XML results"
                }
            elif format_const in [N3, TURTLE]:
                result_data = results.convert()
                formatted_result = {
                    "format": "turtle",
                    "data": str(result_data),
                    "info": "RDF results"
                }
            elif format_const in [CSV, TSV]:
                result_data = results.convert()
                formatted_result = {
                    "format": "tabular",
                    "data": str(result_data),
                    "info": f"{format_const} results"
                }
            else:
                result_data = results.convert()
                formatted_result = {
                    "format": "unknown",
                    "data": str(result_data),
                    "info": "Raw results"
                }
                
            result = {
                "success": True,
                "execution_time": execution_time,
                "endpoint": endpoint,
                "query_size": len(sparql_query),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "results": formatted_result
            }
            if use_cache and cache_entry:
                self.result_cache.save(
                    sparql_query, 
                    self.query_prefix,
                    {
                        "result": result,
                        "timestamp": time.time()
                    }
                )
            return result
        except Exception as e:
            error_message = f"Error executing SPARQL query: {str(e)}"
            logger.error(error_message)
            return {
                "success": False,
                "error": error_message,
                "endpoint": endpoint,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

    def _format_json_results(self, result_data: Dict[str, Any], sparql_query: str) -> Dict[str, Any]:
        """
        Format JSON results into a more usable structure.
        
        Args:
            result_data: Raw JSON results from the SPARQL endpoint
            sparql_query: The original SPARQL query
            
        Returns:
            Formatted results
        """
        # Determine query type from the query
        query_type = "SELECT"  # Default
        if "ASK" in sparql_query.upper():
            query_type = "ASK"
        elif "CONSTRUCT" in sparql_query.upper():
            query_type = "CONSTRUCT"
        elif "DESCRIBE" in sparql_query.upper():
            query_type = "DESCRIBE"
        
        # Format based on query type
        if query_type == "ASK":
            # ASK queries return a boolean
            if "boolean" in result_data:
                return {
                    "format": "boolean",
                    "boolean": result_data["boolean"],
                    "info": f"Query returned: {result_data['boolean']}"
                }
            else:
                return {
                    "format": "unknown",
                    "data": result_data,
                    "info": "Unexpected ASK query result format"
                }
                
        elif query_type in ["CONSTRUCT", "DESCRIBE"]:
            # CONSTRUCT/DESCRIBE queries return triples
            if "head" in result_data and "results" in result_data and "bindings" in result_data["results"]:
                # Some endpoints might return CONSTRUCT results in SELECT format
                return self._format_select_results(result_data)
            else:
                # Expected format for CONSTRUCT/DESCRIBE is a set of triples
                triples = []
                
                # Try to extract triples from various possible formats
                # This is a simplified approach, actual implementation would depend on the endpoint
                
                if isinstance(result_data, dict) and "results" in result_data:
                    triples_data = result_data["results"].get("bindings", [])
                    for binding in triples_data:
                        subject = binding.get("subject", {}).get("value", "")
                        predicate = binding.get("predicate", {}).get("value", "")
                        object_val = binding.get("object", {}).get("value", "")
                        
                        if subject and predicate and object_val:
                            triples.append({
                                "subject": subject,
                                "predicate": predicate,
                                "object": object_val
                            })
                
                return {
                    "format": "triples",
                    "triples": triples,
                    "count": len(triples),
                    "info": f"Query returned {len(triples)} triples"
                }
                
        else:  # SELECT query
            return self._format_select_results(result_data)
    
    def _format_select_results(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format SELECT query results.
        
        Args:
            result_data: Raw JSON results from the SPARQL endpoint
            
        Returns:
            Formatted SELECT results
        """
        # Extract variables (column names)
        variables = result_data.get("head", {}).get("vars", [])
        
        # Extract bindings (rows)
        bindings = result_data.get("results", {}).get("bindings", [])
        
        # Format rows
        rows = []
        for binding in bindings:
            row = {}
            for var in variables:
                if var in binding:
                    value = binding[var].get("value", "")
                    datatype = binding[var].get("datatype", "")
                    type_info = binding[var].get("type", "")
                    
                    # Format based on type
                    if type_info == "uri":
                        row[var] = {
                            "value": value,
                            "type": "uri"
                        }
                    elif type_info == "literal":
                        row[var] = {
                            "value": value,
                            "type": "literal",
                            "datatype": datatype
                        }
                    else:
                        row[var] = {
                            "value": value,
                            "type": type_info
                        }
                else:
                    row[var] = None
            
            rows.append(row)
        
        return {
            "format": "bindings",
            "variables": variables,
            "rows": rows,
            "count": len(rows),
            "info": f"Query returned {len(rows)} results with variables: {', '.join(variables)}"
        }
    
    def clear_cache(self):
        """Clear the result cache."""
        self.result_cache = {}
        logger.info("Query result cache cleared")
    
    def set_endpoint(
        self, 
        endpoint_url: str, 
        auth_token: Optional[str] = None,
        default_graph: Optional[str] = None
    ):
        """
        Set or update the SPARQL endpoint configuration.
        
        Args:
            endpoint_url: URL of the SPARQL endpoint
            auth_token: Authentication token for the endpoint
            default_graph: Default graph URI
        """
        self.endpoint_url = endpoint_url
        
        if auth_token:
            self.auth_token = auth_token
            
        if default_graph:
            self.default_graph = default_graph
        logger.info(f"SPARQL endpoint updated: {endpoint_url}")
