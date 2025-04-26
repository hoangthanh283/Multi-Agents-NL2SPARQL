import json
import time
from typing import Dict, Any

from master.base import DomainMaster
from agents.ontology_mapping import OntologyMappingAgent
from agents.sparql_construction import SparqlConstructionAgent
from agents.sparql_validation import SparqlValidationAgent
from adapters.agent_adapter import AgentAdapter
from utils.logging_utils import setup_logging
from database.ontology_store import OntologyStore

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class QueryDomainMaster(DomainMaster):
    """
    Domain master for Query operations.
    
    Responsibilities:
    - Ontology mapping
    - SPARQL query construction
    - Query validation
    """
    
    def __init__(self, redis_url: str, ontology_store: OntologyStore = None):
        """
        Initialize the Query domain master.
        
        Args:
            redis_url: Redis URL for communication
            ontology_store: Optional ontology store instance
        """
        super().__init__("query", redis_url)
        
        # Initialize ontology store if needed
        self.ontology_store = ontology_store
        if not self.ontology_store:
            try:
                self.ontology_store = OntologyStore()
            except Exception as e:
                logger.error(f"Error initializing OntologyStore: {e}")
                self.ontology_store = None
        
        # Initialize wrapped agents
        try:
            # Ontology mapping agent
            self.ontology_mapping = AgentAdapter(
                agent_instance=OntologyMappingAgent(ontology_store=self.ontology_store),
                agent_type="ontology_mapping"
            )
            logger.info("Ontology mapping agent initialized")
            
            # SPARQL construction agent
            self.sparql_construction = AgentAdapter(
                agent_instance=SparqlConstructionAgent(),
                agent_type="sparql_construction"
            )
            logger.info("SPARQL construction agent initialized")
            
            # SPARQL validation agent
            self.sparql_validation = AgentAdapter(
                agent_instance=SparqlValidationAgent(),
                agent_type="validation"
            )
            logger.info("SPARQL validation agent initialized")
            
        except Exception as e:
            logger.error(f"Error initializing agents in QueryDomainMaster: {e}")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process an incoming Query workflow.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        workflow_data = workflow.get("data", {})
        
        # Check if we have all necessary data from NLP domain
        if "entities" not in workflow_data:
            logger.error(f"Missing entities in workflow {request_id}")
            workflow["error"] = "Missing entities from NLP domain"
            self._complete_workflow(workflow, next_domain=None)
            self.workflow_counter.labels(status="error").inc()
            return
        
        refined_query = workflow_data.get("refined_query", workflow_data.get("query", ""))
        entities = workflow_data.get("entities", {})
        
        logger.info(f"Processing workflow {request_id} with {len(entities)} entities")
        
        # Add this workflow to active workflows
        self.active_workflows[request_id] = {
            "status": "ontology_mapping",
            "tasks": {
                f"{request_id}_ontology_mapping": {
                    "type": "ontology_mapping",
                    "status": "pending"
                }
            },
            "refined_query": refined_query,
            "entities": entities,
            "results": {}
        }
        
        # Start with ontology mapping (first task)
        task_id = f"{request_id}_ontology_mapping"
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "ontology_mapping",
            "parameters": {
                "entities": entities,
                "query_context": refined_query
            }
        }
        
        logger.info(f"Dispatching task {task_id} to ontology mapping slave")
        self._dispatch_to_slave_pool("ontology_mapping", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def process_slave_result(self, result: Dict[str, Any]):
        """
        Process results from Query slaves.
        
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
        
        if slave_type == "ontology_mapping":
            # Store the mapped entities in workflow state
            mapped_entities = task_result.get("mapped_entities", {})
            workflow_state["mapped_entities"] = mapped_entities
            
            # Next step: SPARQL construction
            self._start_sparql_construction(request_id, mapped_entities)
            
        elif slave_type == "sparql_construction":
            # Store SPARQL query in workflow state
            sparql_query = task_result.get("sparql_query", "")
            workflow_state["sparql_query"] = sparql_query
            
            # Next step: SPARQL validation
            self._start_sparql_validation(request_id, sparql_query)
            
        elif slave_type == "validation":
            # Store validation result in workflow state
            validation_result = task_result.get("validation_result", {"valid": False})
            workflow_state["validation_result"] = validation_result
            
            # Complete Query domain work and transfer to Response domain
            self._complete_query_workflow(request_id)
            
        else:
            logger.warning(f"Unhandled slave type in result: {slave_type}")
    
    def _start_sparql_construction(self, request_id: str, mapped_entities: Dict[str, Any]):
        """
        Start SPARQL query construction.
        
        Args:
            request_id: Workflow request ID
            mapped_entities: Mapped entities to use in query construction
        """
        task_id = f"{request_id}_sparql_construction"
        
        # Get the refined query from workflow state
        workflow_state = self.active_workflows[request_id]
        refined_query = workflow_state.get("refined_query", "")
        
        # Create SPARQL construction task
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "sparql_construction",
            "parameters": {
                "mapped_entities": mapped_entities,
                "query_context": refined_query
            }
        }
        
        # Add task to workflow state
        self.active_workflows[request_id]["tasks"][task_id] = {
            "type": "sparql_construction",
            "status": "pending"
        }
        
        # Update workflow status
        self.active_workflows[request_id]["status"] = "sparql_construction"
        
        logger.info(f"Dispatching task {task_id} to SPARQL construction slave")
        self._dispatch_to_slave_pool("sparql_construction", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def _start_sparql_validation(self, request_id: str, sparql_query: str):
        """
        Start SPARQL query validation.
        
        Args:
            request_id: Workflow request ID
            sparql_query: SPARQL query to validate
        """
        task_id = f"{request_id}_sparql_validation"
        
        # Create SPARQL validation task
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "validation",
            "parameters": {
                "query": sparql_query,
                "query_type": "sparql"
            }
        }
        
        # Add task to workflow state
        self.active_workflows[request_id]["tasks"][task_id] = {
            "type": "sparql_validation",
            "status": "pending"
        }
        
        # Update workflow status
        self.active_workflows[request_id]["status"] = "sparql_validation"
        
        logger.info(f"Dispatching task {task_id} to SPARQL validation slave")
        self._dispatch_to_slave_pool("validation", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def _complete_query_workflow(self, request_id: str):
        """
        Complete the Query workflow and prepare for passing to the Response domain.
        
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
            
            # Update workflow data with Query results
            workflow["data"]["sparql_query"] = workflow_state.get("sparql_query", "")
            workflow["data"]["mapped_entities"] = workflow_state.get("mapped_entities", {})
            workflow["data"]["validation_result"] = workflow_state.get("validation_result", {"valid": False})
            
            # Complete this domain's work and forward to response domain
            self._complete_workflow(workflow, next_domain="response")
            
        except Exception as e:
            logger.error(f"Error completing Query workflow {request_id}: {e}")
    
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
        slave_type = task_parts[-1] if len(task_parts) > 1 else "unknown"
        
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
            workflow["error"] = f"Query task failed: {result.get('error', 'Unknown error')}"
            workflow["error_details"] = {
                "domain": "query",
                "task": slave_type,
                "error_type": result.get("error_type", "UnknownError")
            }
            
            # Try to handle failures differently based on the task
            if slave_type == "ontology_mapping":
                # If ontology mapping fails, we can't meaningfully proceed
                # Just set empty mapped entities and try to construct a query anyway
                workflow_state["mapped_entities"] = {
                    "classes": [],
                    "properties": [],
                    "instances": [],
                    "literals": [],
                    "unknown": workflow_state.get("entities", {})
                }
                
                self._start_sparql_construction(request_id, workflow_state["mapped_entities"])
                
            elif slave_type == "sparql_construction":
                # If SPARQL construction fails, we can't proceed with query execution
                # Set an empty SPARQL query and skip validation
                workflow_state["sparql_query"] = ""
                workflow_state["validation_result"] = {"valid": False, "errors": ["Query construction failed"]}
                
                # Complete the workflow with error state
                self._complete_query_workflow(request_id)
                
            elif slave_type == "validation":
                # If validation fails, we still have a query, it's just not validated
                # Proceed to the next domain with the unvalidated query
                workflow_state["validation_result"] = {"valid": False, "errors": ["Validation failed"]}
                self._complete_query_workflow(request_id)
                
            else:
                # For unknown task types, just pass the error along
                self._complete_workflow(workflow, next_domain=None)
                
        except Exception as e:
            logger.error(f"Error handling task failure for {task_id}: {e}")