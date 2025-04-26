import uuid
import json
from typing import Dict, Any

from master.base import DomainMaster
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ResponseDomainMaster(DomainMaster):
    """
    Domain master responsible for coordinating response generation tasks.
    Manages response generation and formatting.
    """
    
    def __init__(self, redis_url: str):
        """
        Initialize the response domain master.
        
        Args:
            redis_url: Redis connection URL
        """
        super().__init__(domain="response", redis_url=redis_url)
        
        # Track task states for each workflow
        self.active_workflows = {}
        
        logger.info("ResponseDomainMaster initialized")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process a workflow in the response domain.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        original_query = workflow.get("data", {}).get("query", "")
        sparql_query = workflow.get("data", {}).get("sparql_query", "")
        query_results = workflow.get("data", {}).get("query_results", {})
        validation_result = workflow.get("data", {}).get("validation_result", {})
        validation_errors = workflow.get("data", {}).get("validation_errors", None)
        
        logger.info(f"ResponseDomainMaster processing workflow {request_id}")
        
        # Track in active workflows
        self.active_workflows[request_id] = {
            "response_generation_complete": False
        }
        
        # Start Response Generation task
        self._start_response_generation(
            request_id, 
            original_query, 
            sparql_query, 
            query_results, 
            validation_result,
            validation_errors
        )
    
    def process_slave_result(self, result: Dict[str, Any]):
        """
        Process results from slave tasks.
        
        Args:
            result: The result data from a slave
        """
        request_id = result.get("request_id")
        slave_type = result.get("slave_type")
        task_id = result.get("task_id")
        success = result.get("success", False)
        
        if request_id not in self.active_workflows:
            logger.warning(f"Received result for unknown workflow {request_id}")
            return
        
        if slave_type == "response_generation":
            self._handle_response_generation_result(request_id, result, success)
        else:
            logger.warning(f"Received result from unknown slave type: {slave_type}")
    
    def _start_response_generation(self, request_id: str, original_query: str, sparql_query: str, 
                                  query_results: Dict[str, Any], validation_result: Dict[str, Any],
                                  validation_errors: str = None):
        """
        Start response generation task.
        
        Args:
            request_id: The workflow request ID
            original_query: The original natural language query
            sparql_query: The constructed SPARQL query
            query_results: Results from executing the SPARQL query
            validation_result: Validation result for the SPARQL query
            validation_errors: Any validation errors
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "original_query": original_query,
                "sparql_query": sparql_query,
                "query_results": query_results,
                "validation_result": validation_result,
                "validation_errors": validation_errors
            }
        }
        
        # Dispatch to response generation slave pool
        self._dispatch_to_slave_pool("response_generation", task)
        logger.info(f"ResponseDomainMaster dispatched response generation task {task_id} for workflow {request_id}")
    
    def _handle_response_generation_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from response generation slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        if not success:
            self._handle_response_error(request_id, "Response generation failed", result.get("error", "Unknown error"))
            return
        
        # Mark response generation as complete
        self.active_workflows[request_id]["response_generation_complete"] = True
        
        # Get the generated response
        response = result.get("response", "")
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["response"] = response
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"ResponseDomainMaster received response for workflow {request_id}")
        
        # Complete workflow
        self._complete_workflow_final(request_id)
    
    def _complete_workflow_final(self, request_id: str):
        """
        Complete the workflow and notify the global master.
        
        Args:
            request_id: The workflow request ID
        """
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            
            # Mark all steps as complete
            for i, step in enumerate(workflow.get("steps", [])):
                if step.get("domain") == self.domain:
                    step["status"] = "complete"
                    step["completed_at"] = self._get_timestamp()
            
            # Mark workflow as complete
            workflow["status"] = "complete"
            workflow["completed_at"] = self._get_timestamp()
            
            # Update workflow in Redis
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
            
            # Notify global master
            self.redis.publish("global:completions", json.dumps(workflow))
            
            # Clean up tracking
            if request_id in self.active_workflows:
                del self.active_workflows[request_id]
                self.active_workflows_gauge.dec()
            
            # Update metrics
            self.workflow_counter.labels(status="complete").inc()
    
    def _handle_response_error(self, request_id: str, error_type: str, error_message: str):
        """
        Handle errors in response generation tasks.
        
        Args:
            request_id: The workflow request ID
            error_type: The type of error
            error_message: The error message
        """
        logger.error(f"Error in workflow {request_id}: {error_type} - {error_message}")
        
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            
            # Mark this domain's step as error
            for i, step in enumerate(workflow.get("steps", [])):
                if step.get("domain") == self.domain:
                    step["status"] = "error"
                    step["error"] = f"{error_type}: {error_message}"
                    break
                    
            # Add error to workflow
            workflow["error"] = f"{error_type}: {error_message}"
            
            # Add a fallback response
            workflow["data"]["response"] = "I'm sorry, I encountered an error processing your request."
            
            # Mark workflow as complete with error
            workflow["status"] = "error"
            workflow["completed_at"] = self._get_timestamp()
            
            # Update workflow in Redis
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
            
            # Notify global master
            self.redis.publish("global:completions", json.dumps(workflow))
            
            # Clean up tracking
            if request_id in self.active_workflows:
                del self.active_workflows[request_id]
                self.active_workflows_gauge.dec()
            
            # Update metrics
            self.workflow_counter.labels(status="error").inc()