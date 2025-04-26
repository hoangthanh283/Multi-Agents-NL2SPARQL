from typing import Dict, Any
import time

from slaves.base import AbstractSlave
from agents.ontology_mapping import OntologyMappingAgent
from database.ontology_store import OntologyStore
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class OntologyMappingSlave(AbstractSlave):
    """
    Slave responsible for mapping entities to ontology concepts.
    Wraps the existing OntologyMappingAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the ontology mapping slave.
        
        Args:
            config: Configuration dictionary including ontology settings
        """
        self.config = config or {}
        
        # Initialize ontology store
        ontology_config = self.config.get("ontology_config", {})
        
        try:
            # Initialize ontology store with available configuration
            ontology_store = OntologyStore(**ontology_config) if ontology_config else OntologyStore()
            
            # Initialize the existing agent
            self.agent = OntologyMappingAgent(
                ontology_store=ontology_store
            )
        except Exception as e:
            logger.error(f"Error initializing OntologyMappingSlave: {e}")
            # Create a placeholder agent that will be properly initialized later
            self.agent = None
        
        # Metrics
        self.task_counter = Counter(
            'ontology_mapping_tasks_total',
            'Total ontology mapping tasks processed',
            ['status']
        )
        self.processing_time = Histogram(
            'ontology_mapping_processing_seconds',
            'Time spent processing ontology mapping tasks'
        )
        self.entity_counter = Counter(
            'ontology_mapping_entities_total',
            'Total entities mapped to ontology concepts',
            ['mapped_status']
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.total_entities_mapped = 0
        self.total_entities_unmapped = 0
        self.start_time = time.time()
        
        logger.info("OntologyMappingSlave initialized")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map entities to ontology concepts.
        
        Args:
            parameters: Task parameters including entities and query context
            
        Returns:
            Mapped entities
        """
        start_time = time.time()
        try:
            entities = parameters.get("entities", [])
            query_context = parameters.get("query_context", "")
            
            if not entities:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing entities parameter"
                }
            
            # Check if agent is initialized
            if not self.agent:
                self.task_counter.labels(status="error").inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Ontology mapping agent not initialized properly"
                }
            
            # Execute ontology mapping using the agent
            mapped_entities = self.agent.map_entities(entities, query_context)
            
            # Count mapped vs. unmapped entities for metrics
            mapped_count = 0
            unmapped_count = 0
            
            for entity in mapped_entities:
                if entity.get("mapped_to"):
                    mapped_count += 1
                else:
                    unmapped_count += 1
            
            # Update metrics
            self.task_counter.labels(status="success").inc()
            self.total_processed += 1
            self.successful_tasks += 1
            self.total_entities_mapped += mapped_count
            self.total_entities_unmapped += unmapped_count
            self.entity_counter.labels(mapped_status="mapped").inc(mapped_count)
            self.entity_counter.labels(mapped_status="unmapped").inc(unmapped_count)
            
            return {
                "success": True,
                "mapped_entities": mapped_entities
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error").inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in OntologyMappingSlave: {e}")
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
        total_entities = self.total_entities_mapped + self.total_entities_unmapped
        mapping_rate = self.total_entities_mapped / max(1, total_entities) * 100
        
        return {
            "type": "ontology_mapping",
            "status": "active" if self.agent else "degraded",
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.successful_tasks / max(1, self.total_processed) * 100,
            "total_entities": total_entities,
            "mapped_entities": self.total_entities_mapped,
            "unmapped_entities": self.total_entities_unmapped,
            "mapping_rate": mapping_rate
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return self.agent is not None and hasattr(self.agent, 'map_entities')