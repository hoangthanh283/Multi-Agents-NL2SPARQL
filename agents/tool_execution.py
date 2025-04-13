from typing import Any, Dict, List, Optional

from agents.query_execution import QueryExecutionAgent


class ToolExecutionAgent:
    """
    Slave agent responsible for executing SPARQL queries.
    This is a wrapper around QueryExecutionAgent for architectural consistency
    with the original blockchain project's Master-Slave pattern.
    """
    
    def __init__(
        self, 
        endpoint_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        default_graph: Optional[str] = None
    ):
        """
        Initialize the tool execution agent.
        
        Args:
            endpoint_url: URL of the SPARQL endpoint
            auth_token: Authentication token for the endpoint
            default_graph: Default graph URI
        """
        # Initialize the underlying query execution agent
        self.query_executor = QueryExecutionAgent(
            endpoint_url=endpoint_url,
            auth_token=auth_token,
            default_graph=default_graph
        )
    
    def execute_tools(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute SPARQL queries according to the execution plan.
        This is a direct mapping to the execute_sparql_queries method of QueryExecutionAgent.
        
        Args:
            execution_plan: The execution plan containing SPARQL queries to execute
            
        Returns:
            Dictionary of execution results
        """
        # If plan has no steps, return empty results
        if "steps" not in execution_plan or not execution_plan["steps"]:
            return {
                "success": False,
                "message": execution_plan.get("message", "No execution steps provided.")
            }
        
        # Extract SPARQL queries from the plan
        sparql_steps = []
        
        for step in execution_plan["steps"]:
            if step.get("action") == "execute_sparql":
                sparql_steps.append({
                    "step_number": step.get("step_number"),
                    "sparql": step.get("sparql", ""),
                    "endpoint": step.get("endpoint", None)
                })
        
        # If no SPARQL steps, return error
        if not sparql_steps:
            return {
                "success": False,
                "message": "No SPARQL queries found in execution plan."
            }
        
        # Execute all SPARQL queries in the plan
        results = {}
        
        for step in sparql_steps:
            step_num = step["step_number"]
            
            # Execute the query
            query_result = self.query_executor.execute_query(
                sparql_query=step["sparql"],
                endpoint_url=step.get("endpoint")
            )
            
            # Store the result
            results[f"step_{step_num}"] = query_result
        
        return {
            "success": True,
            "results": results
        }
    
    def execute_single_query(
        self, 
        sparql_query: str, 
        endpoint_url: Optional[str] = None,
        result_format: str = "json",
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a single SPARQL query.
        Direct passthrough to QueryExecutionAgent's execute_query method.
        
        Args:
            sparql_query: The SPARQL query to execute
            endpoint_url: Optional URL to override the default endpoint
            result_format: Format for the results (json, xml, etc.)
            use_cache: Whether to use cached results if available
            
        Returns:
            Query execution results
        """
        return self.query_executor.execute_query(
            sparql_query=sparql_query,
            endpoint_url=endpoint_url,
            result_format=result_format,
            use_cache=use_cache
        )
    
    def clear_cache(self):
        """Clear the query result cache."""
        self.query_executor.clear_cache()
    
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
        self.query_executor.set_endpoint(
            endpoint_url=endpoint_url,
            auth_token=auth_token,
            default_graph=default_graph
        )