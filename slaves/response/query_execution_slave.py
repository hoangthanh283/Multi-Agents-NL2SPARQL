from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.query_execution import QueryExecutionAgent
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram, Gauge

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryExecutionSlave(AbstractSlave):
    """
    Slave responsible for executing SPARQL queries against a knowledge graph.
    Wraps the existing QueryExecutionAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the query execution slave.
        
        Args:
            config: Configuration dictionary including endpoint settings
        """
        self.config = config or {}
        
        # Configure endpoint information
        endpoint_url = self.config.get("endpoint_url")
        auth_config = self.config.get("auth_config", {})
        connection_pool_size = self.config.get("connection_pool_size", 5)
        timeout = self.config.get("timeout", 30)
        
        try:
            # Initialize the existing agent with available configuration
            self.agent = QueryExecutionAgent(
                endpoint_url=endpoint_url,
                auth_config=auth_config,
                connection_pool_size=connection_pool_size,
                timeout=timeout
            )
        except Exception as e:
            logger.error(f"Error initializing QueryExecutionSlave: {e}")
            # Create a placeholder agent that will be properly initialized later
            self.agent = None
        
        # Metrics
        self.task_counter = Counter(
            'query_execution_tasks_total',
            'Total query execution tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'query_execution_processing_seconds',
            'Time spent processing query execution tasks'
        )
        self.result_size = Histogram(
            'query_execution_result_size',
            'Size of query execution results (number of records)'
        )
        self.active_queries = Gauge(
            'query_execution_active_queries',
            'Number of queries currently being executed'
        )
        
        # Stats
        self.total_executed = 0
        self.successful_executions = 0
        self.failed_executions = 0
        self.total_results = 0
        self.start_time = time.time()
        
        logger.info("QueryExecutionSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a SPARQL query.
        
        Args:
            parameters: Task parameters including the SPARQL query and metadata
            
        Returns:
            Query execution results
        """
        start_time = time.time()
        self.active_queries.inc()
        
        try:
            sparql_query = parameters.get("sparql_query", "")
            query_metadata = parameters.get("query_metadata", {})
            
            if not sparql_query:
                self.task_counter.labels(status="error").inc()
                self.failed_executions += 1
                return {
                    "success": False,
                    "error": "Missing required parameter: sparql_query"
                }
            
            # Check if agent is initialized
            if not self.agent:
                self.task_counter.labels(status="error").inc()
                self.failed_executions += 1
                return {
                    "success": False,
                    "error": "Query execution agent not initialized properly"
                }
            
            # Execute query using the agent
            timeout_override = query_metadata.get("timeout_override")
            results = self.agent.execute_query(
                sparql_query, 
                timeout=timeout_override
            )
            
            # Update metrics and stats
            self.task_counter.labels(status="success").inc()
            self.total_executed += 1
            self.successful_executions += 1
            
            num_results = len(results.get("bindings", [])) if isinstance(results, dict) and "bindings" in results else 0
            self.total_results += num_results
            self.result_size.observe(num_results)
            
            return {
                "success": True,
                "results": results
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_executions += 1
            
            logger.error(f"Error in QueryExecutionSlave: {e}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            # Record processing time and decrease active queries count
            self.processing_time.observe(time.time() - start_time)
            self.active_queries.dec()
    
    def report_status(self) -> Dict[str, Any]:
        """
        Report the current status of this slave.
        
        Returns:
            Dictionary with status information
        """
        uptime = time.time() - self.start_time
        
        return {
            "type": "query_execution",
            "status": "active" if self.agent else "degraded",
            "uptime_seconds": uptime,
            "total_executed": self.total_executed,
            "successful_executions": self.successful_executions,
            "failed_executions": self.failed_executions,
            "success_rate": self.successful_executions / max(1, self.total_executed) * 100,
            "total_results": self.total_results,
            "avg_results_per_query": self.total_results / max(1, self.successful_executions)
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return self.agent is not None and hasattr(self.agent, 'execute_query')