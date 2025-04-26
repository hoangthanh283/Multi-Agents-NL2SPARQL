from typing import Dict, Any, Callable
import time
from prometheus_client import Counter, Histogram
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class AgentAdapter:
    """
    Adapter to use existing agents as slaves in the new architecture.
    This class wraps agent instances and provides a standardized interface 
    for interacting with them in the master-slave architecture.
    """
    
    def __init__(self, agent_instance, agent_type: str, agent_method: str = None):
        """
        Initialize the agent adapter.
        
        Args:
            agent_instance: The instance of the agent to wrap
            agent_type: Type of the agent (e.g., 'entity_recognition', 'ontology_mapping')
            agent_method: The method name to call on the agent (defaults to a method name based on agent_type)
        """
        self.agent = agent_instance
        self.agent_type = agent_type
        
        # Default method mapping based on agent type if not explicitly provided
        if agent_method is None:
            method_mappings = {
                "entity_recognition": "recognize_entities",
                "query_refinement": "refine_query",
                "ontology_mapping": "map_entities",
                "sparql_construction": "construct_query",
                "query_execution": "execute_query",
                "response_generation": "generate_response",
                "validation": "validate"
            }
            self.agent_method = method_mappings.get(agent_type, "execute")
        else:
            self.agent_method = agent_method
        
        # Ensure the agent has the required method
        if not hasattr(self.agent, self.agent_method):
            logger.warning(f"Agent of type {agent_type} does not have method {self.agent_method}")
        
        # Prometheus metrics
        self.task_counter = Counter(
            f'agent_tasks_total', 
            'Total tasks processed by agent',
            ['agent_type', 'status']
        )
        self.execution_time = Histogram(
            f'agent_execution_seconds', 
            'Time spent in agent execution',
            ['agent_type']
        )
        
        # Statistics
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.total_processed = 0
        self.start_time = time.time()
        
        logger.info(f"AgentAdapter initialized for agent type {agent_type} using method {self.agent_method}")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a task using the wrapped agent.
        
        Args:
            parameters: Task parameters specific to the agent type
            
        Returns:
            Dictionary with task execution results
        """
        start_time = time.time()
        try:
            # Get the method to call
            method = getattr(self.agent, self.agent_method)
            
            # Adapt parameters based on agent type
            if self.agent_type == "entity_recognition":
                result = method(parameters.get("query", ""))
                response = {"entities": result}
            
            elif self.agent_type == "query_refinement":
                result = method(
                    parameters.get("query", ""), 
                    parameters.get("context", [])
                )
                response = {"refined_query": result}
            
            elif self.agent_type == "ontology_mapping":
                result = method(
                    parameters.get("entities", {}), 
                    parameters.get("query_context", "")
                )
                response = {"mapped_entities": result}
            
            elif self.agent_type == "sparql_construction":
                result = method(
                    parameters.get("mapped_entities", {}), 
                    parameters.get("query_context", "")
                )
                response = {"sparql_query": result}
            
            elif self.agent_type == "query_execution":
                result = method(
                    parameters.get("sparql_query", ""),
                    parameters.get("endpoint", None)
                )
                response = {"results": result}
                
            elif self.agent_type == "response_generation":
                result = method(
                    parameters.get("query_results", {}),
                    parameters.get("original_query", "")
                )
                response = {"response": result}
                
            else:
                # Generic method call using parameters as kwargs
                result = method(**parameters)
                response = {"result": result}
            
            # Update metrics and stats
            self.task_counter.labels(agent_type=self.agent_type, status="success").inc()
            self.successful_tasks += 1
            self.total_processed += 1
            
            return {"success": True, "result": response}
            
        except Exception as e:
            # Log error and update metrics
            logger.error(f"Error executing {self.agent_type} agent task: {e}")
            self.task_counter.labels(agent_type=self.agent_type, status="error").inc()
            self.failed_tasks += 1
            self.total_processed += 1
            
            return {"success": False, "error": str(e), "error_type": type(e).__name__}
        finally:
            # Record execution time
            execution_time = time.time() - start_time
            self.execution_time.labels(agent_type=self.agent_type).observe(execution_time)
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of this adapter.
        
        Returns:
            Dictionary with status information
        """
        uptime = time.time() - self.start_time
        success_rate = self.successful_tasks / max(1, self.total_processed) * 100
        
        return {
            "agent_type": self.agent_type,
            "agent_method": self.agent_method,
            "uptime_seconds": uptime,
            "total_processed": self.total_processed,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": success_rate
        }
    
    def is_healthy(self) -> bool:
        """
        Check if this adapter is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return (
            self.agent is not None and 
            hasattr(self.agent, self.agent_method) and
            (self.total_processed == 0 or self.successful_tasks / self.total_processed >= 0.7)
        )