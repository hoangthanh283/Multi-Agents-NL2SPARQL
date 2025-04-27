import time
import uuid
from typing import Any, Dict, List

from prometheus_client import CollectorRegistry, Counter, Histogram

from adapters.agent_adapter import AgentAdapter
from agents.ontology_mapping import OntologyMappingAgent
from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class OntologyMappingSlave(AbstractSlave):
    """
    Slave responsible for mapping recognized entities to ontology concepts.
    Wraps the existing OntologyMappingAgent through an adapter.
    """
    
    def __init__(self, config: Dict[str, Any] = None, registry: CollectorRegistry = None):
        """
        Initialize the ontology mapping slave.
        
        Args:
            config: Configuration dictionary
            registry: Optional custom Prometheus registry to avoid metric conflicts
        """
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]  # Generate a unique ID for this instance
        self.registry = registry  # Use provided registry or default
        
        try:
            # Initialize the ontology mapping agent
            agent = OntologyMappingAgent()
            
            # Wrap the agent with an adapter
            self.agent_adapter = AgentAdapter(
                agent_instance=agent,
                agent_type="ontology_mapping"
            )
            
        except Exception as e:
            logger.error(f"Error initializing OntologyMappingSlave: {e}")
            self.agent_adapter = None
        
        # Metrics with unique instance ID to prevent conflicts
        self.task_counter = Counter(
            'ontology_mapping_tasks_total',
            'Total ontology mapping tasks processed',
            ['status', 'instance'],
            registry=self.registry
        )
        self.processing_time = Histogram(
            'ontology_mapping_processing_seconds',
            'Time spent processing ontology mapping tasks',
            ['instance'],
            registry=self.registry
        )
        
        # Stats
        self.total_processed = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
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
            entities = parameters.get("entities", {})
            query_context = parameters.get("query_context", "")
            
            if not entities:
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Missing entities parameter"
                }
            
            # Check if agent adapter is initialized
            if not self.agent_adapter:
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return {
                    "success": False,
                    "error": "Ontology mapping agent adapter not initialized properly"
                }
            
            # Execute ontology mapping using the agent adapter
            result = self.agent_adapter.execute_task({
                "entities": entities,
                "query_context": query_context
            })
            
            if not result.get("success", False):
                self.task_counter.labels(status="error", instance=self.instance_id).inc()
                self.failed_tasks += 1
                return result
                
            mapped_entities = result.get("result", {}).get("mapped_entities", {})
            
            # Update metrics
            self.task_counter.labels(status="success", instance=self.instance_id).inc()
            self.total_processed += 1
            self.successful_tasks += 1
            
            return {
                "success": True,
                "mapped_entities": mapped_entities
            }
        except Exception as e:
            # Update error metrics
            self.task_counter.labels(status="error", instance=self.instance_id).inc()
            self.failed_tasks += 1
            
            logger.error(f"Error in OntologyMappingSlave: {e}")
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
            "type": "ontology_mapping",
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