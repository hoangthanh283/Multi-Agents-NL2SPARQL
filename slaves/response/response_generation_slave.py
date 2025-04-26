from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.response_generation import ResponseGenerationAgent
from adapters.agent_adapter import AgentAdapter
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ResponseGenerationSlave(AbstractSlave):
    """
    Slave responsible for generating natural language responses from SPARQL query results.
    Wraps the existing ResponseGenerationAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the response generation slave.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        
        try:
            # Initialize the response generation agent
            agent = ResponseGenerationAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="response_generation"
            )
            
        except Exception as e:
            logger.error(f"Error initializing ResponseGenerationSlave: {e}")
            self.agent_adapter = None
        
        # Metrics
        self.task_counter = Counter(
            'response_generation_tasks_total',
            'Total response generation tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'response_generation_processing_seconds',
            'Time spent processing response generation tasks'
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.start_time = time.time()
        
        logger.info("ResponseGenerationSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a natural language response from query results.
        
        Args:
            parameters: Task parameters including query results and original query
            
        Returns:
            Generated response
        """
        start_time = time.time()
        try:
            query_results = parameters.get("query_results", {})
            original_query = parameters.get("original_query", "")
            
            if not original_query:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing original query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Response generation agent adapter not initialized properly"
                }
            
            # Execute response generation using the agent adapter
            result = self.agent_adapter.execute_task({
                "query_results": query_results,
                "original_query": original_query
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return result
                
            response = result.get("result", {}).get("response", "")
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "response": response
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in ResponseGenerationSlave: {e}")
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
            "type": "response_generation",
            "status": "active" if self.agent_adapter else "degraded",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100,
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