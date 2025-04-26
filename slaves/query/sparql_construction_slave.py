from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.sparql_construction import SPARQLConstructionAgent
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class SPARQLConstructionSlave(AbstractSlave):
    """
    Slave responsible for constructing SPARQL queries.
    Wraps the existing SPARQLConstructionAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the SPARQL construction slave.
        
        Args:
            config: Configuration dictionary (optional)
        """
        self.config = config or {}
        
        # Initialize the existing agent
        self.agent = SPARQLConstructionAgent()
        
        # Metrics
        self.task_counter = Counter(
            'sparql_construction_tasks_total',
            'Total SPARQL construction tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'sparql_construction_processing_seconds',
            'Time spent processing SPARQL construction tasks'
        )
        self.query_counter = Counter(
            'sparql_queries_total',
            'Total SPARQL queries constructed',
            ['query_type']
        )
        
        # Stats
        self.total_processed = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.fix_attempts = 0
        self.start_time = time.time()
        
        logger.info("SPARQLConstructionSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construct a SPARQL query based on the query plan and mapped entities.
        
        Args:
            parameters: Task parameters including refined query, mapped entities, and query plan
            
        Returns:
            Generated SPARQL query and metadata
        """
        start_time = time.time()
        try:
            refined_query = parameters.get("refined_query", "")
            mapped_entities = parameters.get("mapped_entities", {})
            query_plan = parameters.get("query_plan", {})
            fix_attempt = parameters.get("fix_attempt", False)
            validation_feedback = parameters.get("validation_feedback", "")
            previous_query = parameters.get("previous_query", "")
            
            if not refined_query or not mapped_entities:
                self.task_counter.labels(status="error").inc()
                self.failed_queries += 1
                return {
                    "success": False,
                    "error": "Missing required parameters: refined_query or mapped_entities"
                }
            
            # Track if this is a fix attempt
            if fix_attempt:
                self.fix_attempts += 1
            
            # Construct SPARQL query using the agent
            if fix_attempt and previous_query and validation_feedback:
                sparql_query, metadata = self.agent.construct_query(
                    refined_query, 
                    mapped_entities, 
                    query_plan,
                    fix=True,
                    validation_feedback=validation_feedback,
                    previous_query=previous_query
                )
            else:
                sparql_query, metadata = self.agent.construct_query(
                    refined_query, 
                    mapped_entities, 
                    query_plan
                )
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_queries += 1
            
            # Track query type in metrics
            query_type = metadata.get("query_type", "unknown")
            self.query_counter.labels(query_type=query_type).inc()
            
            return {
                "success": True,
                "sparql": sparql_query,
                "metadata": metadata
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_queries += 1
            
            logger.error(f"Error in SPARQLConstructionSlave: {e}")
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
            "type": "sparql_construction",
            "status": "active",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_queries": self.successful_queries,
            "failed_queries": self.failed_queries,
            "fix_attempts": self.fix_attempts,
            "success_rate": self.successful_queries / max(1, self.total_processed) * 100
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return hasattr(self, 'agent') and hasattr(self.agent, 'construct_query')