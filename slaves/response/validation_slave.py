import importlib
import time
from typing import Any, Dict

from prometheus_client import Counter, Histogram

from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ValidationSlave(AbstractSlave):
    """
    Slave responsible for validating SPARQL queries.
    Wraps the existing SPARQLValidationAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the validation slave.
        
        Args:
            config: Configuration dictionary (optional)
        """
        self.config = config or {}
        
        # Dynamically import the SPARQLValidationAgent to handle both versions
        try:
            # Try to import the newer version first
            validation_module = importlib.import_module('agents.validation_2')
            self.agent = validation_module.SPARQLValidationAgent()
            self.version = 2
        except (ImportError, AttributeError):
            # Fall back to the original version
            validation_module = importlib.import_module('agents.sparql_validation')
            self.agent = validation_module.SPARQLValidationAgent()
            self.version = 1
        
        # Metrics
        self.task_counter = Counter(
            'validation_tasks_total',
            'Total validation tasks processed',
            ['status', 'version', 'valid']
        )
        self.processing_time = Histogram(
            'validation_processing_seconds',
            'Time spent processing validation tasks',
            ['version']
        )
        
        # Stats
        self.total_validated = 0
        self.valid_queries = 0
        self.invalid_queries = 0
        self.start_time = time.time()
        
        logger.info(f"ValidationSlave initialized using version {self.version}")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a SPARQL query.
        
        Args:
            parameters: Task parameters including the SPARQL query
            
        Returns:
            Validation results
        """
        start_time = time.time()
        try:
            sparql_query = parameters.get("sparql_query", "")
            query_metadata = parameters.get("query_metadata", {})
            
            if not sparql_query:
                self.task_counter.labels(status="error", version=self.version, valid="unknown").inc()
                return {
                    "success": False,
                    "error": "Missing required parameter: sparql_query"
                }
            
            # Execute validation using the agent
            validation_result = self.agent.validate(
                sparql_query, 
                query_metadata=query_metadata
            )
            
            # Get validation status
            is_valid = validation_result.get("is_valid", False)
            status_label = "valid" if is_valid else "invalid"
            
            # Update metrics and stats
            self.task_counter.labels(status="success", version=self.version, valid=status_label).inc()
            self.total_validated += 1
            if is_valid:
                self.valid_queries += 1
            else:
                self.invalid_queries += 1
            
            return {
                "success": True,
                "is_valid": is_valid,
                "feedback": validation_result.get("feedback", ""),
                "can_execute": validation_result.get("can_execute", False),
                "suggestions": validation_result.get("suggestions", [])
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error", version=self.version, valid="unknown").inc()
            
            logger.error(f"Error in ValidationSlave: {e}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            # Record processing time
            self.processing_time.labels(version=self.version).observe(time.time() - start_time)
    
    def report_status(self) -> Dict[str, Any]:
        """
        Report the current status of this slave.
        
        Returns:
            Dictionary with status information
        """
        uptime = time.time() - self.start_time
        validation_rate = self.valid_queries / max(1, self.total_validated) * 100
        
        return {
            "type": "validation",
            "version": self.version,
            "status": "active",
            "uptime_seconds": uptime,
            "total_validated": self.total_validated,
            "valid_queries": self.valid_queries,
            "invalid_queries": self.invalid_queries,
            "validation_rate": validation_rate
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return hasattr(self, 'agent') and hasattr(self.agent, 'validate')