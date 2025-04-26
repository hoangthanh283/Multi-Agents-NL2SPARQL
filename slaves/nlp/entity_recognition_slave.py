from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.entity_recognition import EntityRecognitionAgent
from models.entity_recognition import GLiNERModel
from database.ontology_store import OntologyStore
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class EntityRecognitionSlave(AbstractSlave):
    """
    Slave responsible for recognizing entities in natural language queries.
    Wraps the existing EntityRecognitionAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the entity recognition slave.
        
        Args:
            config: Configuration dictionary including model paths and ontology config
        """
        self.config = config or {}
        
        # Initialize model and ontology store
        model_path = self.config.get("model_path", None)
        ontology_config = self.config.get("ontology_config", {})
        
        try:
            # Initialize components with available configuration
            model = GLiNERModel(model_path=model_path) if model_path else GLiNERModel()
            ontology_store = OntologyStore(**ontology_config) if ontology_config else OntologyStore()
            
            # Initialize the existing agent
            self.agent = EntityRecognitionAgent(
                entity_recognition_model=model,
                ontology_store=ontology_store
            )
        except Exception as e:
            logger.error(f"Error initializing EntityRecognitionSlave: {e}")
            # Create a placeholder agent that will be properly initialized later
            self.agent = None
        
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
        self.total_entities = 0
        self.start_time = time.time()
        
        logger.info("EntityRecognitionSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recognize entities in a natural language query.
        
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
            
            # Check if agent is initialized
            if not self.agent:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Entity recognition agent not initialized properly"
                }
            
            # Execute entity recognition using the agent
            entities = self.agent.recognize_entities(query)
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            self.total_entities += len(entities)
            
            # Track entity types in metrics
            for entity_type in set(entity.get("type", "unknown") for entity in entities):
                self.entity_counter.labels(entity_type=entity_type).inc()
            
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
        
        return {
            "type": "entity_recognition",
            "status": "active" if self.agent else "degraded",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100,
            "total_entities_recognized": self.total_entities,
            "avg_entities_per_query": self.total_entities / max(1, self.successful_tasks)
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return self.agent is not None and hasattr(self.agent, 'recognize_entities')