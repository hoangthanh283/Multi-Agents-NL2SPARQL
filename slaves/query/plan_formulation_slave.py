import importlib
import time
from typing import Any, Dict

from prometheus_client import Counter, Histogram

from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class PlanFormulationSlave(AbstractSlave):
    """
    Slave responsible for formulating query plans.
    Adapts the existing PlanFormulationAgent to the AbstractSlave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the plan formulation slave.
        
        Args:
            config: Configuration dictionary (optional)
        """
        self.config = config or {}
        
        # Dynamically import the PlanFormulationAgent to handle both versions
        try:
            # Try to import the newer version first
            plan_formulation_module = importlib.import_module('agents.plan_formulation_2')
            self.agent = plan_formulation_module.PlanFormulationAgent()
            self.version = 2
        except (ImportError, AttributeError):
            # Fall back to the original version
            plan_formulation_module = importlib.import_module('agents.plan_formulation')
            self.agent = plan_formulation_module.PlanFormulationAgent()
            self.version = 1
        
        # Metrics
        self.task_counter = Counter(
            'plan_formulation_tasks_total',
            'Total plan formulation tasks processed',
            ['status', 'version']
        )
        self.processing_time = Histogram(
            'plan_formulation_processing_seconds',
            'Time spent processing plan formulation tasks',
            ['version']
        )
        
        # Stats
        self.total_plans = 0
        self.successful_plans = 0
        self.failed_plans = 0
        self.start_time = time.time()
        
        logger.info(f"PlanFormulationSlave initialized using version {self.version}")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Formulate a query plan.
        
        Args:
            parameters: Task parameters including refined query and mapped entities
            
        Returns:
            Generated query plan
        """
        start_time = time.time()
        try:
            refined_query = parameters.get("refined_query", "")
            mapped_entities = parameters.get("mapped_entities", {})
            
            if not refined_query:
                self.task_counter.labels(status="error", version=self.version).inc()
                self.failed_plans += 1
                return {
                    "success": False,
                    "error": "Missing required parameter: refined_query"
                }
            
            # Generate query plan using the agent
            plan = self.agent.formulate_plan(refined_query, mapped_entities)
            
            # Update metrics and stats
            self.task_counter.labels(status="success", version=self.version).inc()
            self.total_plans += 1
            self.successful_plans += 1
            
            return {
                "success": True,
                "plan": plan
            }
        except Exception as e:
            # Update error metrics and stats
            self.task_counter.labels(status="error", version=self.version).inc()
            self.failed_plans += 1
            
            logger.error(f"Error in PlanFormulationSlave: {e}")
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
        
        return {
            "type": "plan_formulation",
            "version": self.version,
            "status": "active",
            "uptime_seconds": uptime,
            "total_plans": self.total_plans,
            "successful_plans": self.successful_plans,
            "failed_plans": self.failed_plans,
            "success_rate": self.successful_plans / max(1, self.total_plans) * 100
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return hasattr(self, 'agent') and hasattr(self.agent, 'formulate_plan')