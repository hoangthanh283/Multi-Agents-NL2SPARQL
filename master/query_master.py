import uuid
import json
from typing import Dict, Any

from master.base import DomainMaster
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryDomainMaster(DomainMaster):
    """
    Domain master responsible for coordinating query-related tasks.
    Manages ontology mapping, SPARQL construction, and validation slaves.
    """
    
    def __init__(self, redis_url: str):
        """
        Initialize the query domain master.
        
        Args:
            redis_url: Redis connection URL
        """
        super().__init__(domain="query", redis_url=redis_url)
        
        # Track task states for each workflow
        self.active_workflows = {}
        
        logger.info("QueryDomainMaster initialized")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process a workflow in the query domain.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        refined_query = workflow.get("data", {}).get("refined_query", "")
        entities = workflow.get("data", {}).get("entities", {})
        
        logger.info(f"QueryDomainMaster processing workflow {request_id}")
        
        # Track in active workflows
        self.active_workflows[request_id] = {
            "ontology_mapping_complete": False,
            "sparql_construction_complete": False,
            "validation_complete": False,
            "sparql_query": None,
            "validation_result": None
        }
        
        # Step 1: Start Ontology Mapping task
        self._start_ontology_mapping(request_id, refined_query, entities)
    
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
        
        if slave_type == "ontology_mapping":
            self._handle_ontology_mapping_result(request_id, result, success)
        elif slave_type == "sparql_construction":
            self._handle_sparql_construction_result(request_id, result, success)
        elif slave_type == "sparql_validation":
            self._handle_validation_result(request_id, result, success)
        else:
            logger.warning(f"Received result from unknown slave type: {slave_type}")
    
    def _start_ontology_mapping(self, request_id: str, refined_query: str, entities: Dict[str, Any]):
        """
        Start ontology mapping task.
        
        Args:
            request_id: The workflow request ID
            refined_query: The refined query
            entities: Recognized entities from NLP domain
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "query": refined_query,
                "entities": entities
            }
        }
        
        # Dispatch to ontology mapping slave pool
        self._dispatch_to_slave_pool("ontology_mapping", task)
        logger.info(f"QueryDomainMaster dispatched ontology mapping task {task_id} for workflow {request_id}")
    
    def _handle_ontology_mapping_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from ontology mapping slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        if not success:
            self._handle_query_error(request_id, "Ontology mapping failed", result.get("error", "Unknown error"))
            return
        
        # Mark ontology mapping as complete
        self.active_workflows[request_id]["ontology_mapping_complete"] = True
        
        # Get the mapped ontology entities
        ontology_mappings = result.get("ontology_mappings", {})
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["ontology_mappings"] = ontology_mappings
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"QueryDomainMaster received ontology mappings for workflow {request_id}")
        
        # Start SPARQL construction task
        self._start_sparql_construction(request_id, ontology_mappings)
    
    def _start_sparql_construction(self, request_id: str, ontology_mappings: Dict[str, Any]):
        """
        Start SPARQL construction task.
        
        Args:
            request_id: The workflow request ID
            ontology_mappings: Ontology mappings from previous step
        """
        # Get workflow data
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if not workflow_json:
            self._handle_query_error(request_id, "Workflow not found", "Cannot retrieve workflow data")
            return
            
        workflow = json.loads(workflow_json)
        refined_query = workflow.get("data", {}).get("refined_query", "")
        entities = workflow.get("data", {}).get("entities", {})
        
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "query": refined_query,
                "entities": entities,
                "ontology_mappings": ontology_mappings
            }
        }
        
        # Dispatch to SPARQL construction slave pool
        self._dispatch_to_slave_pool("sparql_construction", task)
        logger.info(f"QueryDomainMaster dispatched SPARQL construction task {task_id} for workflow {request_id}")
    
    def _handle_sparql_construction_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from SPARQL construction slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        if not success:
            self._handle_query_error(request_id, "SPARQL construction failed", result.get("error", "Unknown error"))
            return
        
        # Mark SPARQL construction as complete
        self.active_workflows[request_id]["sparql_construction_complete"] = True
        
        # Get the constructed SPARQL query
        sparql_query = result.get("sparql_query", "")
        self.active_workflows[request_id]["sparql_query"] = sparql_query
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["sparql_query"] = sparql_query
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"QueryDomainMaster received SPARQL query for workflow {request_id}")
        
        # Start validation task
        self._start_validation(request_id, sparql_query)
    
    def _start_validation(self, request_id: str, sparql_query: str):
        """
        Start SPARQL validation task.
        
        Args:
            request_id: The workflow request ID
            sparql_query: The constructed SPARQL query
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "parameters": {
                "sparql_query": sparql_query
            }
        }
        
        # Dispatch to validation slave pool
        self._dispatch_to_slave_pool("sparql_validation", task)
        logger.info(f"QueryDomainMaster dispatched validation task {task_id} for workflow {request_id}")
    
    def _handle_validation_result(self, request_id: str, result: Dict[str, Any], success: bool):
        """
        Handle result from validation slave.
        
        Args:
            request_id: The workflow request ID
            result: The task result data
            success: Whether the task was successful
        """
        # Mark validation as complete regardless of success
        # Even if validation fails, we still want to forward the query 
        # (with validation errors) to the response domain
        self.active_workflows[request_id]["validation_complete"] = True
        
        # Get validation result
        validation_result = result.get("validation_result", {})
        self.active_workflows[request_id]["validation_result"] = validation_result
        
        # Update workflow in Redis
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            workflow["data"]["validation_result"] = validation_result
            
            # If validation failed but we have a query, we'll forward it anyway
            # Response domain will handle explaining validation issues to the user
            if not success and "error" not in workflow:
                workflow["data"]["validation_errors"] = result.get("error", "Validation failed")
                
            self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"QueryDomainMaster received validation result for workflow {request_id}")
        
        # Check if all query tasks are complete
        if self._is_workflow_complete(request_id):
            self._forward_to_response_domain(request_id)
    
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
        return (
            status["ontology_mapping_complete"] and 
            status["sparql_construction_complete"] and 
            status["validation_complete"]
        )
    
    def _forward_to_response_domain(self, request_id: str):
        """
        Forward workflow to the response domain.
        
        Args:
            request_id: The workflow request ID
        """
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if workflow_json:
            workflow = json.loads(workflow_json)
            
            # Get query execution results if validation passed
            status = self.active_workflows[request_id]
            validation_passed = status["validation_result"].get("valid", False) if status["validation_result"] else False
            
            if validation_passed and status["sparql_query"]:
                # Execute the query and add results to workflow data
                try:
                    # Query execution is simplified here - in a real implementation,
                    # you might want to add a query execution slave
                    sparql_query = status["sparql_query"]
                    # Mock some results for demonstration
                    query_results = {"results": ["Sample result 1", "Sample result 2"]}
                    workflow["data"]["query_results"] = query_results
                except Exception as e:
                    logger.error(f"Error executing query for workflow {request_id}: {str(e)}")
                    workflow["data"]["query_execution_error"] = str(e)
            
            # Complete this domain's workflow and forward to response domain
            self._complete_workflow(workflow, next_domain="response")
            
            # Clean up tracking
            if request_id in self.active_workflows:
                del self.active_workflows[request_id]
    
    def _handle_query_error(self, request_id: str, error_type: str, error_message: str):
        """
        Handle errors in query tasks.
        
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