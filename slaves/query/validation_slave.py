import time
from typing import Any, Dict

from prometheus_client import Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.sparql_validation import SparqlValidationAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ValidationSlave(AbstractSlave):
    """
    Slave responsible for validating SPARQL queries.
    Wraps the existing SparqlValidationAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the validation slave.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        
        try:
            # Initialize the validation agent
            agent = SparqlValidationAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="validation"
            )
            
        except Exception as e:
            logger.error(f"Error initializing ValidationSlave: {e}")
            self.agent_adapter = None
        
        # Metrics
        self.task_counter = Counter(
            'validation_tasks_total',
            'Total validation tasks processed',
            ['status', 'valid']
        )
        self.processing_time = Histogram(
            'validation_processing_seconds',
            'Time spent processing validation tasks'
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.valid_queries = 0
        self.invalid_queries = 0
        self.start_time = time.time()
        
        logger.info("ValidationSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a query.
        
        Args:
            parameters: Task parameters including the query and query type
            
        Returns:
            Validation result
        """
        start_time = time.time()
        try:
            query = parameters.get("query", "")
            query_type = parameters.get("query_type", "sparql")
            
            if not query:
                self.task_counter.labels(status="error", valid="unknown").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error", valid="unknown").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Validation agent adapter not initialized properly"
                }
            
            # Execute validation using the agent adapter
            result = self.agent_adapter.execute_task({
                "query": query,
                "query_type": query_type
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error", valid="unknown").inc()
                self.failed_tasks += 1
                return result
                
            validation_result = result.get("result", {}).get("validation_result", {})
            is_valid = validation_result.get("valid", False)
            
            # Update metrics and stats
            self.task_counter.labels(status="success", valid=str(is_valid).lower()).inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            if is_valid:
                self.valid_queries += 1
            else:
                self.invalid_queries += 1
            
            return {
                "success": True,
                "validation_result": validation_result
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error", valid="unknown").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in ValidationSlave: {e}")
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
            "type": "validation",
            "status": "active" if self.agent_adapter else "degraded",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100,
            "valid_queries": self.valid_queries,
            "invalid_queries": self.invalid_queries,
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