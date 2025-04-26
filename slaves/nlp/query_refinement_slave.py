import time
from typing import Any, Dict, List

from prometheus_client import Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.query_refinement import QueryRefinementAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryRefinementSlave(AbstractSlave):
    """
    Slave responsible for refining natural language queries.
    Wraps the existing QueryRefinementAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the query refinement slave.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        
        try:
            # Initialize the query refinement agent
            agent = QueryRefinementAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="query_refinement"
            )
            
        except Exception as e:
            logger.error(f"Error initializing QueryRefinementSlave: {e}")
            self.agent_adapter = None
        
        # Metrics
        self.task_counter = Counter(
            'query_refinement_tasks_total',
            'Total query refinement tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'query_refinement_processing_seconds',
            'Time spent processing query refinement tasks'
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.start_time = time.time()
        
        logger.info("QueryRefinementSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Refine a natural language query.
        
        Args:
            parameters: Task parameters including the query and context
            
        Returns:
            Refined query
        """
        start_time = time.time()
        try:
            query = parameters.get("query", "")
            context = parameters.get("context", [])
            
            if not query:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Query refinement agent adapter not initialized properly"
                }
            
            # Execute query refinement using the agent adapter
            result = self.agent_adapter.execute_task({
                "query": query,
                "context": context
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return result
                
            refined_query = result.get("result", {}).get("refined_query", query)
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "refined_query": refined_query
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in QueryRefinementSlave: {e}")
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
            "type": "query_refinement",
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