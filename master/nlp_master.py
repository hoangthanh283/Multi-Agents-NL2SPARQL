import json
import time
from typing import Any, Dict

from adapters.agent_adapter import AgentAdapter
from agents.entity_recognition import EntityRecognitionAgent
from agents.query_refinement import QueryRefinementAgent
from database.ontology_store import OntologyStore
from database.qdrant_client import QdrantClient
from master.base import DomainMaster
from models.entity_recognition import GLiNERModel
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class NLPDomainMaster(DomainMaster):
    """
    Domain master for Natural Language Processing operations.
    
    Responsibilities:
    - Query refinement
    - Entity recognition
    - Initial NLP preprocessing
    """
    
    def __init__(self, redis_url: str, ontology_store: OntologyStore, qdrant_client: QdrantClient = None):
        """
        Initialize the NLP domain master.
        
        Args:
            redis_url: Redis URL for communication
            ontology_store: OntologyStore instance for entity recognition
            qdrant_client: QdrantClient instance for entity embedding storage
        """
        super().__init__("nlp", redis_url, qdrant_client)
        
        # Store ontology store
        self.ontology_store = ontology_store
        
        # Initialize wrapped agents
        try:
            # Query refinement agent
            self.query_refinement = AgentAdapter(
                agent_instance=QueryRefinementAgent(
                    qdrant_client=qdrant_client
                ),
                agent_type="query_refinement"
            )
            logger.info("Query refinement agent initialized")
            
            # Entity recognition agent - only pass the parameters it expects
            gliner_model = GLiNERModel()
            self.entity_recognition = AgentAdapter(
                agent_instance=EntityRecognitionAgent(
                    entity_recognition_model=gliner_model, 
                    ontology_store=ontology_store
                ),
                agent_type="entity_recognition"
            )
            logger.info("Entity recognition agent initialized")
            
        except Exception as e:
            logger.error(f"Error initializing agents in NLPDomainMaster: {e}")
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process an incoming NLP workflow.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        workflow_data = workflow.get("data", {})
        query = workflow_data.get("query", "")
        
        if not query:
            logger.error(f"Missing query in workflow {request_id}")
            workflow["error"] = "Missing query"
            self._complete_workflow(workflow, next_domain=None)
            self.workflow_counter.labels(status="error").inc()
            return
        
        logger.info(f"Processing workflow {request_id} with query: {query}")
        
        # Add this workflow to active workflows
        self.active_workflows[request_id] = {
            "status": "refining",
            "tasks": {
                f"{request_id}_query_refinement": {
                    "type": "query_refinement",
                    "status": "pending"
                }
            },
            "query": query,
            "results": {}
        }
        
        # Start with query refinement (first task)
        task_id = f"{request_id}_query_refinement"
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "query_refinement",
            "parameters": {
                "query": query,
                "context": workflow_data.get("context", [])
            }
        }
        
        logger.info(f"Dispatching task {task_id} to query refinement slave")
        self._dispatch_to_slave_pool("query_refinement", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def process_slave_result(self, result: Dict[str, Any]):
        """
        Process results from NLP slaves.
        
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
        
        if slave_type == "query_refinement":
            # Store the refined query in workflow state
            refined_query = task_result.get("refined_query", workflow_state["query"])
            workflow_state["refined_query"] = refined_query
            
            # Next step: entity recognition
            self._start_entity_recognition(request_id, refined_query)
            
        elif slave_type == "entity_recognition":
            # Store entities in workflow state
            entities = task_result.get("entities", {})
            workflow_state["entities"] = entities
            
            # Complete NLP domain work and transfer to Query domain
            self._complete_nlp_workflow(request_id)
            
        else:
            logger.warning(f"Unhandled slave type in result: {slave_type}")
    
    def _start_entity_recognition(self, request_id: str, query: str):
        """
        Start entity recognition for the given query.
        
        Args:
            request_id: Workflow request ID
            query: Query to process (usually the refined query)
        """
        task_id = f"{request_id}_entity_recognition"
        
        # Create entity recognition task
        task = {
            "task_id": task_id,
            "request_id": request_id,
            "slave_type": "entity_recognition",
            "parameters": {
                "query": query
            }
        }
        
        # Add task to workflow state
        self.active_workflows[request_id]["tasks"][task_id] = {
            "type": "entity_recognition",
            "status": "pending"
        }
        
        # Update workflow status
        self.active_workflows[request_id]["status"] = "entity_recognition"
        
        logger.info(f"Dispatching task {task_id} to entity recognition slave")
        self._dispatch_to_slave_pool("entity_recognition", task)
        
        # Mark workflow as having a pending task
        self.active_workflows[request_id]["tasks"][task_id]["status"] = "dispatched"
    
    def _complete_nlp_workflow(self, request_id: str):
        """
        Complete the NLP workflow and prepare for passing to the Query domain.
        
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
            
            # Update workflow data with NLP results
            workflow["data"]["refined_query"] = workflow_state.get("refined_query", workflow["data"].get("query", ""))
            workflow["data"]["entities"] = workflow_state.get("entities", {})
            
            # Complete this domain's work and forward to query domain
            self._complete_workflow(workflow, next_domain="query")
            
        except Exception as e:
            logger.error(f"Error completing NLP workflow {request_id}: {e}")
    
    def _handle_task_failure(self, request_id: str, task_id: str, result: Dict[str, Any]):
        """
        Handle a task failure.
        
        Args:
            request_id: Workflow request ID
            task_id: Failed task ID
            result: Failure result data
        """
        # Get slave type from task ID
        slave_type = task_id.split("_")[-1] if "_" in task_id else "unknown"
        
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
            workflow["error"] = f"NLP task failed: {result.get('error', 'Unknown error')}"
            workflow["error_details"] = {
                "domain": "nlp",
                "task": slave_type,
                "error_type": result.get("error_type", "UnknownError")
            }
            
            # Try to provide partial results if possible
            if slave_type == "query_refinement":
                # Continue with original query if refinement fails
                workflow_state["refined_query"] = workflow_state["query"]
                workflow["data"]["refined_query"] = workflow_state["query"]
                
                # Try to proceed with entity recognition
                self._start_entity_recognition(request_id, workflow_state["query"])
                
            elif slave_type == "entity_recognition":
                # If entity recognition fails, provide empty entities and try to continue
                workflow_state["entities"] = {}
                workflow["data"]["entities"] = {}
                
                # Attempt to proceed to next domain despite failure
                self._complete_workflow(workflow, next_domain="query")
                
            else:
                # For unknown task types, just pass the error along
                self._complete_workflow(workflow, next_domain=None)
                
        except Exception as e:
            logger.error(f"Error handling task failure for {task_id}: {e}")