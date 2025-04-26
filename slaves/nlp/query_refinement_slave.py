from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.query_refinement import QueryRefinementAgent
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryRefinementSlave(AbstractSlave):
    """
    Slave responsible for refining natural language queries.
    Wraps the existing QueryRefinementAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the query refinement slave.
        
        Args:
            config: Configuration dictionary (optional)
        """
        self.config = config or {}
        
        # Initialize the existing agent
        self.agent = QueryRefinementAgent()
        
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
            
            # Execute query refinement using the agent
            refined_query = self.agent.refine_query(query, context)
            
            # Update metrics and stats
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "refined_query": refined_query
            }
        except Exception as e:
            # Update error metrics and stats
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
        
        return {
            "type": "query_refinement",
            "status": "active",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return hasattr(self, 'agent') and hasattr(self.agent, 'refine_query')