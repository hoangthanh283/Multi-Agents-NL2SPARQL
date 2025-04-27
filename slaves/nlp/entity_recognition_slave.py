import time
from typing import Any, Dict

from prometheus_client import Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.entity_recognition import EntityRecognitionAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class EntityRecognitionSlave(AbstractSlave):
    """
    Slave responsible for recognizing entities in natural language queries.
    Wraps the existing EntityRecognitionAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the entity recognition slave.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        try:
            # Initialize the entity recognition agent
            agent = EntityRecognitionAgent(
                entity_recognition_model=self.config.get("entity_recognition_model"), 
                ontology_store=self.config.get("ontology_store")
            )
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="entity_recognition"
            )
            
        except Exception as e:
            logger.error(f"Error initializing EntityRecognitionSlave: {e}")
            self.agent_adapter = None
        
        # Metrics
        self.task_counter = Counter(
            'entity_recognition_tasks_total',
            'Total entity recognition tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'entity_recognition_processing_seconds',
            'Time spent processing entity recognition tasks'
        )
        self.entity_counter = Counter(
            'entity_recognition_entities_total',
            'Total entities recognized',
            ['entity_type']
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.start_time = time.time()
        
        logger.info("EntityRecognitionSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recognize entities in a query.
        
        Args:
            parameters: Task parameters including the query
            
        Returns:
            Recognized entities
        """
        start_time = time.time()
        try:
            query = parameters.get("query", "")
            
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
                    "error": "Entity recognition agent adapter not initialized properly"
                }
            
            # Execute entity recognition using the agent adapter
            result = self.agent_adapter.execute_task({"query": query})
            
            if not result.get("success", False):
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return result
                
            entities = result.get("result", {}).get("entities", {})
            
            # Count entities by type for metrics
            for entity_type, entity_list in entities.items():
                self.entity_counter.labels(entity_type=entity_type).inc(len(entity_list))
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "entities": entities
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in EntityRecognitionSlave: {e}")
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
            "type": "entity_recognition",
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