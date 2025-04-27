import hashlib
import json
import time
from typing import Any, Dict, Optional

import redis
from SPARQLWrapper import (CSV, JSON, N3, RDFXML, TSV, TURTLE, XML,
                           SPARQLWrapper)
from caches.query_cache import SPARQLQueryCache

# Configure logging
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryExecutionAgent:
    """
    Slave agent responsible for executing SPARQL queries.
    Handles communication with SPARQL endpoints, authentication, and result formatting.
    Uses Redis for distributed caching of query results.
    """
    
    def __init__(
        self, 
        endpoint_url: Optional[str] = None,
        redis_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        default_graph: Optional[str] = None,
        elastic_client: Optional[Any] = None,
        redis_host: Optional[str]=None,
        redis_port: Optional[int]=None,
        redis_ttl: Optional[int]=None
    ):
        """
        Initialize the query execution agent.
        
        Args:
            endpoint_url: URL of the SPARQL endpoint
            redis_url: URL for Redis connection
            auth_token: Authentication token for the endpoint
            default_graph: Default graph URI
            elastic_client: Elasticsearch client for query storage and lookup
        """
        # Initialize endpoint settings
        self.endpoint_url = endpoint_url
        self.auth_token = auth_token
        self.default_graph = default_graph
        
        # Initialize Redis connection if URL provided
        self.redis_client = None
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url)
                logger.info(f"Connected to Redis cache at {redis_url}")
            except Exception as e:
                logger.warning(f"Could not connect to Redis: {e}")
        
        # Cache configuration
        self.cache_expiry = 86400  # 24 hours default cache lifetime
        
        # Default timeout in seconds
        self.timeout = 30
        
        # Default result format
        self.result_format = JSON
        
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
        
        self.elastic_client = elastic_client
        self.result_cache = None
        self.query_prefix = "cache:sparql:"
    
        if not(redis_host) or not(redis_port) or not(redis_ttl):
            self.result_cache = None
        else:
            self.result_cache = SPARQLQueryCache(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_ttl=redis_ttl
            )
        
    def _generate_cache_key(self, endpoint: str, sparql_query: str, result_format: str) -> str:
        """
        Generate a Redis cache key for a SPARQL query.
        
        Args:
            endpoint: The SPARQL endpoint URL
            sparql_query: The SPARQL query
            result_format: The result format
            
        Returns:
            Cache key string
        """
        # Normalize query by removing extra whitespace
        normalized_query = " ".join(sparql_query.split())
        
        # Create a composite key from endpoint, query, and format
        key_content = f"{endpoint}|{normalized_query}|{result_format}"
        
        # Create a hash of the content
        query_hash = hashlib.md5(key_content.encode('utf-8')).hexdigest()
        
        # Use the same cache key prefix as GlobalMaster for consistency
        return f"cache:sparql:{query_hash}"
    
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
        if use_cache and self.result_cache:
            try:
                cache_output = self.result_cache.search(
                    sparql_query, 
                    self.query_prefix
                )
                if cache_output:
                    return cache_output

            except Exception as e:
                logger.warning(f"Error accessing Redis/Elasticsearch cache: {e}")
        
        # Use provided endpoint or default
        endpoint = endpoint_url or self.endpoint_url
        if not endpoint:
            return {
                "success": False,
                "error": "No SPARQL endpoint specified"
            }
        format_const = self.format_map.get(result_format.lower(), JSON)

        # Try to get cached result if caching is enabled
#         if use_cache and self.redis_client:
#             try:
#                 cache_key = self._generate_cache_key(endpoint, sparql_query, result_format)
#                 cached_data = self.redis_client.get(cache_key)
                
#                 if cached_data:
#                     cached_result = json.loads(cached_data)
#                     cached_result["from_cache"] = True
#                     logger.info(f"Redis cache hit for SPARQL query: {sparql_query[:50]}...")
#                     return cached_result
#                 else:
#                     logger.debug(f"Redis cache miss for SPARQL query: {sparql_query[:50]}...")
#                     # Fallback to Elasticsearch if available and user_query is provided
#                     if self.elastic_client and user_query:
#                         es_result = self.elastic_client.search_similar_query(user_query)
#                         if es_result and es_result.get("sparql_query"):
#                             logger.info(f"Elasticsearch hit for user query: {user_query[:50]}...")
#                             return {
#                                 "success": True,
#                                 "from_cache": True,
#                                 "cache_type": "elasticsearch",
#                                 "sparql_query": es_result["sparql_query"],
#                                 "response": es_result.get("response", ""),
#                                 "score": es_result.get("score", 0),
#                                 "timestamp": es_result.get("timestamp", None)
#                             }
        
        
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
            if use_cache and self.result_cache:
                self.result_cache.save(
                    sparql_query, 
                    self.query_prefix,
                    {
                        "result": result,
                        "timestamp": time.time()
                    }
                )
            
            # Cache successful results in Redis
#             if use_cache and self.redis_client:
#                 try:
#                     cache_key = self._generate_cache_key(endpoint, sparql_query, result_format)
#                     self.redis_client.setex(
#                         cache_key,
#                         self.cache_expiry,
#                         json.dumps(result)
#                     )
#                     logger.debug(f"Cached SPARQL result in Redis for query: {sparql_query[:50]}...")
#                 except Exception as e:
#                     logger.warning(f"Error caching result in Redis: {e}")
                    
#             # Store in Elasticsearch if available and user_query is provided
#             if self.elastic_client and user_query:
#                 try:
#                     self.elastic_client.store_query_sparql_pair({
#                         "natural_query": user_query,
#                         "sparql_query": sparql_query,
#                         "response": result.get("results", {}),
#                         "successful": result.get("success", False),
#                         "execution_time": result.get("execution_time", 0),
#                         "timestamp": result.get("timestamp", None),
#                         "context": context or ""
#                     })
#                 except Exception as e:
#                     logger.warning(f"Error storing query-SPARQL pair in Elasticsearch: {e}")
                    
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
    
    def clear_cache(self, endpoint: Optional[str] = None):
        """
        Clear the SPARQL query result cache in Redis.
        
        Args:
            endpoint: Optional endpoint URL to clear specific endpoint cache only
        """
        if not self.redis_client:
            logger.warning("Redis client not initialized, can't clear cache")
            return
            
        try:
            if endpoint:
                # Get keys matching the specific endpoint pattern
                pattern = f"cache:sparql:{hashlib.md5(endpoint.encode('utf-8')).hexdigest()}*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"Cleared {len(keys)} cached SPARQL results for endpoint {endpoint}")
            else:
                # Clear all SPARQL cache
                keys = self.redis_client.keys("cache:sparql:*")
                if keys:
                    self.redis_client.delete(*keys)
                    logger.info(f"Cleared {len(keys)} cached SPARQL results")
        except Exception as e:
            logger.error(f"Error clearing Redis cache: {e}")
    
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
    
    def set_cache_expiry(self, seconds: int):
        """
        Set the cache expiry time in seconds.
        
        Args:
            seconds: Number of seconds before cache entries expire
        """
        self.cache_expiry = seconds
        logger.info(f"SPARQL cache expiry set to {seconds} seconds")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the SPARQL query cache.
        
        Returns:
            Dictionary with cache statistics
        """
        if not self.redis_client:
            return {"error": "Redis client not initialized"}
            
        try:
            # Count all SPARQL cache keys
            keys = self.redis_client.keys("cache:sparql:*")
            
            # Calculate total size of cache entries
            total_size = 0
            if keys:
                pipeline = self.redis_client.pipeline()
                for key in keys:
                    pipeline.strlen(key)
                sizes = pipeline.execute()
                total_size = sum(sizes)
                
            return {
                "entries": len(keys),
                "size_bytes": total_size,
                "size_mb": total_size / (1024 * 1024) if total_size else 0,
                "expiry_seconds": self.cache_expiry
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}
