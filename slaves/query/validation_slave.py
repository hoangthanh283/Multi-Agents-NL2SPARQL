import time
import uuid
from typing import Any, Dict

from prometheus_client import CollectorRegistry, Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.validation import ValidationAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ValidationSlave(AbstractSlave):
    """
    Slave responsible for validating SPARQL queries.
    Wraps the existing ValidationAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None, registry: CollectorRegistry = None):
        """
        Initialize the validation slave.
        
        Args:
            config: Configuration dictionary
            registry: Optional custom Prometheus registry to avoid metric conflicts
        """
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]  # Generate a unique ID for this instance
        self.registry = registry  # Use provided registry or default
        
        try:
            # Initialize the validation agent
            agent = ValidationAgent()
            
            # Wrap the agent with an adapter - Changed method_name to agent_method
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="validation",
                agent_method="validate_query"  # Use the correct method name
            )
            
            logger.info(f"Agent of type validation using validate_query method instead of validate")
            logger.info("ValidationSlave initialized")
            
        except Exception as e:
            logger.error(f"Error initializing ValidationSlave: {e}")
            self.agent_adapter = None
        
        # Metrics with unique instance ID to prevent conflicts
        self.task_counter = Counter(
            'validation_tasks_total',
            'Total validation tasks processed',
            ['status', 'instance'],
            registry=self.registry
        )
        self.processing_time = Histogram(
            'validation_processing_seconds',
            'Time spent processing validation tasks',
            ['instance'],
            registry=self.registry
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.valid_queries = 0
        self.invalid_queries = 0
        self.start_time = time.time()
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a SPARQL query.
        
        Args:
            parameters: Task parameters including the SPARQL query
            
        Returns:
            Validation result
        """
        start_time = time.time()
        try:
            sparql_query = parameters.get("sparql_query", "")
            
            if not sparql_query:
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing sparql_query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Validation agent adapter not initialized properly"
                }
            
            # Execute validation using the agent adapter
            result = self.agent_adapter.execute_task({
                "sparql_query": sparql_query
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return result
            
            is_valid = result.get("result", {}).get("is_valid", False)
            validation_errors = result.get("result", {}).get("errors", [])
            
            # Update metrics
            self.task_counter.labels(status="success", instance=self.instance_id).inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            # Track valid and invalid queries
            if is_valid:
                self.valid_queries += 1
            else:
                self.invalid_queries += 1
            
            return {
                "success": True,
                "is_valid": is_valid,
                "validation_errors": validation_errors
            }
        
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error", instance=self.instance_id).inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in ValidationSlave: {e}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            # Record processing time
            self.processing_time.labels(instance=self.instance_id).observe(time.time() - start_time)
    
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