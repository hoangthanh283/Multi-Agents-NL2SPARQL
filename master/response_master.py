import json
import time
from typing import Any, Dict

from adapters.agent_adapter import AgentAdapter
from agents.query_execution import QueryExecutionAgent
from agents.response_generation import ResponseGenerationAgent
from master.base import DomainMaster
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ResponseDomainMaster(DomainMaster):
    """
    Domain master for Response operations.
    
    Responsibilities:
    - Query execution
    - Response generation
    - Final output formatting
    """
    
    def __init__(self, redis_url: str, endpoint_url: str = None):
        """
        Initialize the Response domain master.
        
        Args:
            redis_url: Redis URL for communication
            endpoint_url: SPARQL endpoint URL for query execution
        """
        super().__init__("response", redis_url)
        
        self.endpoint_url = endpoint_url
        
        # Initialize wrapped agents
        try:
            # Query execution agent with Redis caching support
            self.query_execution = AgentAdapter(
                agent_instance=QueryExecutionAgent(
                    endpoint_url=endpoint_url,
                    redis_url=redis_url  # Pass Redis URL to enable distributed caching
                ),
                agent_type="query_execution"
            )
            logger.info("Query execution agent initialized with Redis caching")
            
            # Response generation agent
            self.response_generation = AgentAdapter(
                agent_instance=ResponseGenerationAgent(),
                agent_type="response_generation"
            )
            logger.info("Response generation agent initialized")
            
        except Exception as e:
            logger.error(f"Error initializing agents in ResponseDomainMaster: {e}")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process an incoming Response workflow.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        workflow_data = workflow.get("data", {})
        
        # Check if we have a SPARQL query from Query domain
        if "sparql_query" not in workflow_data:
            logger.error(f"Missing SPARQL query in workflow {request_id}")
            workflow["error"] = "Missing SPARQL query from Query domain"
            self._complete_workflow(workflow, next_domain=None)
            self.workflow_counter.labels(status="error").inc()
            return
        
        sparql_query = workflow_data.get("sparql_query", "")
        validation_result = workflow_data.get("validation_result", {"valid": False})
        original_query = workflow_data.get("query", "")
        refined_query = workflow_data.get("refined_query", original_query)
        
        # Check if the query is valid before executing it
        if not validation_result.get("valid", False) and not sparql_query:
            logger.warning(f"Invalid SPARQL query in workflow {request_id}")
            # We'll still process, but generate an error response
            
        logger.info(f"Processing workflow {request_id} with SPARQL query: {sparql_query[:100]}...")
        
        # Add this workflow to active workflows
        self.active_workflows[request_id] = {
            "status": "query_execution",
            "tasks": {
                f"{request_id}_query_execution": {
                    "type": "query_execution",
                    "status": "pending"
                }
            },
            "sparql_query": sparql_query,
            "original_query": original_query,
            "refined_query": refined_query,
            "validation_result": validation_result,
            "results": {}
        }
        
        # Start with query execution (first task)
        task_id = f"{request_id}_query_execution"
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "query_execution",
            "parameters": {
                "sparql_query": sparql_query,
                "endpoint": self.endpoint_url,
                "use_cache": True  # Enable caching for all query executions
            }
        }
        
        logger.info(f"Dispatching task {task_id} to query execution slave")
        self._dispatch_to_slave_pool("query_execution", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def process_slave_result(self, result: Dict[str, Any]):
        """
        Process results from Response slaves.
        
        Args:
            result: Result data from a slave
        """
        task_id = result.get("task_id", "")
        request_id = result.get("request_id", "")
        
        # Check if this is a result we're expecting
        if request_id not in self.active_workflows:
            logger.warning(f"Received result for unknown workflow {request_id}")
            return
            
        workflow_state = self.active_workflows[request_id]
        
        # Update task status in our workflow state
        if task_id in workflow_state["tasks"]:
            workflow_state["tasks"][task_id]["status"] = "completed"
        else:
            logger.warning(f"Received result for unknown task {task_id}")
            return
            
        if not result.get("success", False):
            # Handle task failure
            logger.error(f"Task {task_id} failed: {result.get('error', 'Unknown error')}")
            self._handle_task_failure(request_id, task_id, result)
            return
            
        # Process result based on task type
        slave_type = result.get("slave_type", "unknown")
        task_result = result.get("result", {})
        
        if slave_type == "query_execution":
            # Store the query results in workflow state
            query_results = task_result.get("results", {})
            workflow_state["query_results"] = query_results
            
            # Next step: response generation
            self._start_response_generation(request_id, query_results)
            
        elif slave_type == "response_generation":
            # Store the generated response in workflow state
            response = task_result.get("response", "")
            workflow_state["response"] = response
            
            # Complete Response domain work and finish the workflow
            self._complete_response_workflow(request_id)
            
        else:
            logger.warning(f"Unhandled slave type in result: {slave_type}")
    
    def _start_response_generation(self, request_id: str, query_results: Dict[str, Any]):
        """
        Start response generation based on query results.
        
        Args:
            request_id: Workflow request ID
            query_results: Results from executing the SPARQL query
        """
        task_id = f"{request_id}_response_generation"
        
        # Get the original query from workflow state
        workflow_state = self.active_workflows[request_id]
        original_query = workflow_state.get("original_query", "")
        
        # Create response generation task
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "response_generation",
            "parameters": {
                "query_results": query_results,
                "original_query": original_query
            }
        }
        
        # Add task to workflow state
        self.active_workflows[request_id]["tasks"][task_id] = {
            "type": "response_generation",
            "status": "pending"
        }
        
        # Update workflow status
        self.active_workflows[request_id]["status"] = "response_generation"
        
        logger.info(f"Dispatching task {task_id} to response generation slave")
        self._dispatch_to_slave_pool("response_generation", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def _complete_response_workflow(self, request_id: str):
        """
        Complete the Response workflow and finish the overall workflow.
        
        Args:
            request_id: Workflow request ID
        """
        # Get workflow state
        workflow_state = self.active_workflows.get(request_id, {})
        
        # Retrieve workflow from Redis to update it
        workflow_key = f"workflow:{request_id}"
        try:
            workflow_json = self.redis.get(workflow_key)
            if not workflow_json:
                logger.error(f"Workflow {request_id} not found in Redis")
                return
                
            workflow = json.loads(workflow_json)
            
            # Update workflow data with Response results
            workflow["data"]["query_results"] = workflow_state.get("query_results", {})
            workflow["data"]["response"] = workflow_state.get("response", "")
            workflow["completed_at"] = time.time()
            
            # Complete this domain's work and mark the workflow as complete
            self._complete_workflow(workflow, next_domain=None)
            
        except Exception as e:
            logger.error(f"Error completing Response workflow {request_id}: {e}")
    
    def _handle_task_failure(self, request_id: str, task_id: str, result: Dict[str, Any]):
        """
        Handle a task failure.
        
        Args:
            request_id: Workflow request ID
            task_id: Failed task ID
            result: Failure result data
        """
        # Get slave type from task ID
        task_parts = task_id.split("_")
        slave_type = "_".join(task_parts[1:]) if len(task_parts) > 1 else "unknown"
        
        # Get workflow state
        workflow_state = self.active_workflows.get(request_id, {})
        
        # Retrieve workflow from Redis
        workflow_key = f"workflow:{request_id}"
        try:
            workflow_json = self.redis.get(workflow_key)
            if not workflow_json:
                logger.error(f"Workflow {request_id} not found in Redis")
                return
                
            workflow = json.loads(workflow_json)
            
            # Add error information to workflow
            workflow["error"] = f"Response task failed: {result.get('error', 'Unknown error')}"
            workflow["error_details"] = {
                "domain": "response",
                "task": slave_type,
                "error_type": result.get("error_type", "UnknownError")
            }
            
            # Try to handle failures differently based on the task
            if slave_type == "query_execution":
                # If query execution fails, use empty results
                # and continue with response generation
                workflow_state["query_results"] = {
                    "success": False,
                    "error": result.get("error", "Query execution failed"),
                    "results": []
                }
                
                # Try to generate an error response
                self._start_response_generation(request_id, workflow_state["query_results"])
                
            elif slave_type == "response_generation":
                # If response generation fails, use a default error response
                workflow_state["response"] = f"I'm sorry, but I couldn't generate a response based on your query. Error: {result.get('error', 'Unknown error')}"
                workflow["data"]["response"] = workflow_state["response"]
                
                # Complete the workflow with the error response
                self._complete_workflow(workflow, next_domain=None)
                
            else:
                # For unknown task types, just pass the error along
                self._complete_workflow(workflow, next_domain=None)
                
        except Exception as e:
            logger.error(f"Error handling task failure for {task_id}: {e}")