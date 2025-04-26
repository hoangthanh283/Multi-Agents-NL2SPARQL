import uuid
import json
from typing import Dict, Any

from master.base import DomainMaster
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class NLPDomainMaster(DomainMaster):
    """
    Domain master responsible for coordinating NLP-related tasks.
    Manages query refinement and entity recognition slaves.
    """
    
    def __init__(self, redis_url: str):
        """
        Initialize the NLP domain master.
        
        Args:
            redis_url: Redis connection URL
        """
        super().__init__(domain="nlp", redis_url=redis_url)
        
        # Track task states for each workflow
        self.workflow_tasks = {}
        
        logger.info("NLPDomainMaster initialized")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process a workflow in the NLP domain.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        query = workflow.get("data", {}).get("query", "")
        context = workflow.get("data", {}).get("context", [])
        
        logger.info(f"NLPDomainMaster processing workflow {request_id}")
        
        # Track in active workflows
        self.active_workflows[request_id] = {
            "query_refinement_complete": False,
            "entity_recognition_complete": False
        }
        
        # Step 1: Start Query Refinement task
        self._start_query_refinement(request_id, query, context)
    
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
        
        if slave_type == "query_refinement":
            self._handle_query_refinement_result(request_id, result, success)
        elif slave_type == "entity_recognition":
            self._handle_entity_recognition_result(request_id, result, success)
        else:
            logger.warning(f"Received result from unknown slave type: {slave_type}")
    
    def _start_query_refinement(self, request_id: str, query: str, context: list):
        """
        Start query refinement task.
        
        Args:
            request_id: The workflow request ID
            query: The original natural language query
            context: Optional context for the query
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "query": query,
                "context": context
            }
        }
        
        # Dispatch to query refinement slave pool
        self._dispatch_to_slave_pool("query_refinement", task)
        logger.info(f"NLPDomainMaster dispatched query refinement task {task_id} for workflow {request_id}")
    
    def _handle_query_refinement_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from query refinement slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        if not success:
            self._handle_nlp_error(request_id, "Query refinement failed", result.get("error", "Unknown error"))
            return
        
        # Mark query refinement as complete
        self.active_workflows[request_id]["query_refinement_complete"] = True
        
        # Get the refined query
        refined_query = result.get("refined_query", "")
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["refined_query"] = refined_query
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"NLPDomainMaster received refined query for workflow {request_id}")
        
        # Start entity recognition task
        self._start_entity_recognition(request_id, refined_query)
    
    def _start_entity_recognition(self, request_id: str, refined_query: str):
        """
        Start entity recognition task.
        
        Args:
            request_id: The workflow request ID
            refined_query: The refined natural language query
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "query": refined_query
            }
        }
        
        # Dispatch to entity recognition slave pool
        self._dispatch_to_slave_pool("entity_recognition", task)
        logger.info(f"NLPDomainMaster dispatched entity recognition task {task_id} for workflow {request_id}")
    
    def _handle_entity_recognition_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from entity recognition slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        if not success:
            self._handle_nlp_error(request_id, "Entity recognition failed", result.get("error", "Unknown error"))
            return
        
        # Mark entity recognition as complete
        self.active_workflows[request_id]["entity_recognition_complete"] = True
        
        # Get the recognized entities
        entities = result.get("entities", {})
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["entities"] = entities
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"NLPDomainMaster received entities for workflow {request_id}")
        
        # Check if all NLP tasks are complete
        if self._is_workflow_complete(request_id):
            self._forward_to_query_domain(request_id)
    
    def _is_workflow_complete(self, request_id: str) -> bool:
        """
        Check if all tasks for a workflow are complete.
        
        Args:
            request_id: The workflow request ID
            
        Returns:
            Boolean indicating if all tasks are complete
        """
        if request_id not in self.active_workflows:
            return False
            
        status = self.active_workflows[request_id]
        return status["query_refinement_complete"] and status["entity_recognition_complete"]
    
    def _forward_to_query_domain(self, request_id: str):
        """
        Forward workflow to the query domain.
        
        Args:
            request_id: The workflow request ID
        """
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            
            # Complete this domain's workflow and forward to query domain
            self._complete_workflow(workflow, next_domain="query")
            
            # Clean up tracking
            if request_id in self.active_workflows:
                del self.active_workflows[request_id]
    
    def _handle_nlp_error(self, request_id: str, error_type: str, error_message: str):
        """
        Handle errors in NLP tasks.
        
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
            
            # Update workflow in Redis
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
            
            # Send completion to global master
            self.redis.publish("global:completions", json.dumps(workflow))
            
            # Clean up tracking
            if request_id in self.active_workflows:
                del self.active_workflows[request_id]
                self.active_workflows_gauge.dec()
            
            # Update metrics
            self.workflow_counter.labels(status="error").inc()