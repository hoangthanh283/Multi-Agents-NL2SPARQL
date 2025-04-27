import time
import uuid
from typing import Any, Dict, List, Optional

from prometheus_client import CollectorRegistry, Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.entity_recognition import EntityRecognitionAgent
from database.ontology_store import OntologyStore
from models.entity_recognition import EntityRecognitionModel
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class EntityRecognitionSlave(AbstractSlave):
    """
    Slave responsible for recognizing and extracting entities from natural language.
    Wraps the existing EntityRecognitionAgent through an adapter.
    """
    
    def __init__(
        self, 
        config: Dict[str, Any] = None, 
        registry: CollectorRegistry = None,
        entity_recognition_model: Optional[EntityRecognitionModel] = None,
        ontology_store: Optional[OntologyStore] = None
    ):
        """
        Initialize the entity recognition slave.
        
        Args:
            config: Configuration dictionary
            registry: Optional custom Prometheus registry to avoid metric conflicts
            entity_recognition_model: Entity recognition model
            ontology_store: Ontology store for entity mapping
        """
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]  # Generate a unique ID for this instance
        self.registry = registry  # Use provided registry or default
        
        try:
            # Try to get dependencies from config if not provided directly
            if entity_recognition_model is None and "entity_recognition_model" in self.config:
                entity_recognition_model = self.config["entity_recognition_model"]
                
            if ontology_store is None and "ontology_store" in self.config:
                ontology_store = self.config["ontology_store"]
            
            # Initialize the entity recognition agent
            if entity_recognition_model and ontology_store:
                agent = EntityRecognitionAgent(
                    entity_recognition_model=entity_recognition_model,
                    ontology_store=ontology_store
                )
            else:
                # Initialize with mock/default implementations if dependencies are not available
                agent = EntityRecognitionAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="entity_recognition"
            )
            
        except Exception as e:
            logger.error(f"Error initializing EntityRecognitionSlave: {e}")
            self.agent_adapter = None
        
        # Metrics with unique instance ID to prevent conflicts
        self.task_counter = Counter(
            'entity_recognition_tasks_total',
            'Total entity recognition tasks processed',
            ['status', 'instance'],
            registry=self.registry
        )
        self.processing_time = Histogram(
            'entity_recognition_processing_seconds',
            'Time spent processing entity recognition tasks',
            ['instance'],
            registry=self.registry
        )
        self.entity_counter = Counter(
            'entity_recognition_entities',
            'Number of entities recognized by type',
            ['entity_type', 'instance'],
            registry=self.registry
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.recognized_entities = 0
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
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing query parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Entity recognition agent adapter not initialized properly"
                }
            
            # Execute entity recognition using the agent adapter
            result = self.agent_adapter.execute_task({"query": query})
            
            if not result.get("success", False):
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return result
                
            entities = result.get("result", {}).get("entities", {})
            
            # Count entities by type for metrics
            entity_count = 0
            for entity_type, entity_list in entities.items():
                entity_count += len(entity_list)
                self.entity_counter.labels(entity_type=entity_type, instance=self.instance_id).inc(len(entity_list))
            
            # Update metrics and stats
            self.recognized_entities += entity_count
            self.task_counter.labels(status="success", instance=self.instance_id).inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "entities": entities
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error", instance=self.instance_id).inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in EntityRecognitionSlave: {e}")
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
