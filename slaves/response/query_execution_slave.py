import time
from typing import Any, Dict

from prometheus_client import Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.query_execution import QueryExecutionAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

# Metrics (module-level, not per-instance)
query_execution_task_counter = Counter(
    'query_execution_tasks_total',
    'Total query execution tasks processed',
    ['status']
)
query_execution_processing_time = Histogram(
    'query_execution_processing_seconds',
    'Time spent processing query execution tasks'
)

class QueryExecutionSlave(AbstractSlave):
    """
    Slave responsible for executing SPARQL queries against an endpoint.
    Wraps the existing QueryExecutionAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the query execution slave.
        
        Args:
            config: Configuration dictionary including endpoint settings
        """
        self.config = config or {}
        
        # Get default endpoint from config if provided
        self.default_endpoint = self.config.get("endpoint_url")
        
        try:
            # Initialize the query execution agent
            agent = QueryExecutionAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="query_execution"
            )
            
        except Exception as e:
            logger.error(f"Error initializing QueryExecutionSlave: {e}")
            self.agent_adapter = None
        
        # Metrics (reference module-level)
        self.task_counter = query_execution_task_counter
        self.processing_time = query_execution_processing_time
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.start_time = time.time()
        
        logger.info("QueryExecutionSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a SPARQL query against an endpoint.
        
        Args:
            parameters: Task parameters including SPARQL query and endpoint
            
        Returns:
            Query execution results
        """
        start_time = time.time()
        try:
            sparql_query = parameters.get("sparql_query", "")
            endpoint = parameters.get("endpoint", self.default_endpoint)
            
            if not sparql_query:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing SPARQL query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Query execution agent adapter not initialized properly"
                }
            
            # Execute SPARQL query using the agent adapter
            result = self.agent_adapter.execute_task({
                "sparql_query": sparql_query,
                "endpoint": endpoint
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return result
                
            query_results = result.get("result", {}).get("results", {})
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "results": query_results
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in QueryExecutionSlave: {e}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            # Record processing time
            self.processing_time.observe(time.time() - start_time)
    
    def report_status(self) -> Dict[str, Any]:
        """
        Report the current status of this slave.
        
        Returns:
            Dictionary with status information
        """
        uptime = time.time() - self.start_time
        
        # Include adapter status if available
        adapter_status = self.agent_adapter.get_status() if self.agent_adapter else {"status": "unavailable"}
        
        return {
            "type": "query_execution",
            "status": "active" if self.agent_adapter else "degraded",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100,
            "default_endpoint": self.default_endpoint,
            "adapter": adapter_status
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return (
            self.agent_adapter is not None and 
            self.agent_adapter.is_healthy()
        )