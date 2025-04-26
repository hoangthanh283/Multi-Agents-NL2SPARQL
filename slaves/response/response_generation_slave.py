from typing import Dict, Any
import time
import importlib

from slaves.base import AbstractSlave
from utils.logging_utils import setup_logging
from prometheus_client import Counter, Histogram

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ResponseGenerationSlave(AbstractSlave):
    """
    Slave responsible for generating natural language responses from query results.
    Wraps the existing ResponseGenerationAgent to adapt it to the slave interface.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the response generation slave.
        
        Args:
            config: Configuration dictionary (optional)
        """
        self.config = config or {}
        
        # Dynamically import the ResponseGenerationAgent to handle both versions
        try:
            # Try to import the newer version first
            response_generation_module = importlib.import_module('agents.response_generation_2')
            self.agent = response_generation_module.ResponseGenerationAgent()
            self.version = 2
        except (ImportError, AttributeError):
            # Fall back to the original version
            response_generation_module = importlib.import_module('agents.response_generation')
            self.agent = response_generation_module.ResponseGenerationAgent()
            self.version = 1
        
        # Metrics
        self.task_counter = Counter(
            'response_generation_tasks_total',
            'Total response generation tasks processed',
            ['status', 'version']
        )
        self.processing_time = Histogram(
            'response_generation_processing_seconds',
            'Time spent processing response generation tasks',
            ['version']
        )
        self.response_length = Histogram(
            'response_generation_length',
            'Length of generated responses in characters',
            ['status']  # 'success' or 'error'
        )
        
        # Stats
        self.total_responses = 0
        self.successful_responses = 0
        self.error_responses = 0
        self.start_time = time.time()
        
        logger.info(f"ResponseGenerationSlave initialized using version {self.version}")
    
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a natural language response from query results.
        
        Args:
            parameters: Task parameters including refined query, SPARQL query, and execution results
            
        Returns:
            Generated natural language response
        """
        start_time = time.time()
        try:
            refined_query = parameters.get("refined_query", "")
            sparql_query = parameters.get("sparql_query", "")
            execution_results = parameters.get("execution_results", {})
            query_metadata = parameters.get("query_metadata", {})
            
            if not refined_query:
                self.task_counter.labels(status="error", version=self.version).inc()
                self.error_responses += 1
                return {
                    "success": False,
                    "error": "Missing required parameter: refined_query"
                }
                
            # Generate response using the agent
            if execution_results.get("success", False) is False:
                # Error case - generate an error response
                error_message = execution_results.get("error", "Unknown error occurred during query execution")
                response = self.agent.generate_error_response(
                    refined_query, 
                    sparql_query, 
                    error_message
                )
            else:
                # Success case - generate a response from results
                response = self.agent.generate_response(
                    refined_query,
                    sparql_query,
                    execution_results.get("results", {}),
                    query_metadata
                )
            
            # Update metrics and stats
            self.task_counter.labels(status="success", version=self.version).inc()
            self.total_responses += 1
            self.successful_responses += 1
            self.response_length.labels(status="success").observe(len(response))
            
            return {
                "success": True,
                "response": response
            }
        except Exception as e:
            # Update error metrics and stats
            self.task_counter.labels(status="error", version=self.version).inc()
            self.error_responses += 1
            
            error_msg = str(e)
            self.response_length.labels(status="error").observe(len(error_msg))
            
            logger.error(f"Error in ResponseGenerationSlave: {e}")
            return {
                "success": False,
                "error": error_msg,
                "response": f"I apologize, but I encountered an error while generating a response: {error_msg}"
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
            "type": "response_generation",
            "version": self.version,
            "status": "active",
            "uptime_seconds": uptime,
            "total_responses": self.total_responses,
            "successful_responses": self.successful_responses,
            "error_responses": self.error_responses,
            "success_rate": self.successful_responses / max(1, self.total_responses) * 100
        }
    
    def get_health(self) -> bool:
        """
        Check if this slave is healthy.
        
        Returns:
            Boolean indicating health status
        """
        return hasattr(self, 'agent') and hasattr(self.agent, 'generate_response')