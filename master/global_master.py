import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

import redis
from prometheus_client import Counter, Gauge, Histogram

from master.nlp_master import NLPDomainMaster
from master.query_master import QueryDomainMaster
from master.response_master import ResponseDomainMaster
from utils.logging_utils import setup_logging
from utils.monitoring import (log_domain_processing, log_workflow_completion,
                              log_workflow_start, metrics_logger)

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class GlobalMaster:
    """
    Global master that coordinates the NL2SPARQL workflow across domains.
    
    The GlobalMaster is responsible for:
    - Creating and initializing workflows
    - Routing workflows between domain masters
    - Handling workflow completion and errors
    - Providing status updates and metrics
    """
    
    def __init__(self, redis_url: str, endpoint_url: str = None):
        """
        Initialize the global master.
        
        Args:
            redis_url: Redis URL for communication
            endpoint_url: SPARQL endpoint URL
        """
        self.redis_url = redis_url
        self.endpoint_url = endpoint_url
        
        # Initialize Redis connections
        self.redis = redis.from_url(redis_url)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        
        # Initialize domain masters
        self.domain_masters = {
            "nlp": NLPDomainMaster(redis_url),
            "query": QueryDomainMaster(redis_url),
            "response": ResponseDomainMaster(redis_url, endpoint_url)
        }
        
        # Keep track of active workflows
        self.active_workflows = {}
        
        # Thread control
        self.running = False
        self.completion_listener_thread = None
        self.domain_transition_thread = None
        
        # Prometheus metrics
        self.request_counter = Counter(
            'nl2sparql_requests_total',
            'Total number of NL2SPARQL requests',
            ['status']
        )
        self.processing_time = Histogram(
            'nl2sparql_processing_seconds',
            'Time spent processing NL2SPARQL requests'
        )
        self.active_workflows_gauge = Gauge(
            'nl2sparql_active_workflows',
            'Number of active NL2SPARQL workflows'
        )
        
        logger.info("GlobalMaster initialized")
    
    def start(self):
        """Start the global master and all domain masters."""
        if self.running:
            return
            
        # Start domain masters
        for domain, master in self.domain_masters.items():
            master.start()
            logger.info(f"Started {domain} domain master")
        
        # Subscribe to completion channel and domain transition channel
        self.pubsub.subscribe("global:completions", "global:domain_transitions")
        
        # Start completion listener thread
        self.running = True
        self.completion_listener_thread = threading.Thread(target=self._listen_for_messages)
        self.completion_listener_thread.daemon = True
        self.completion_listener_thread.start()
        
        logger.info("GlobalMaster started")
    
    def stop(self):
        """Stop the global master and all domain masters."""
        self.running = False
        
        # Stop domain masters
        for domain, master in self.domain_masters.items():
            master.stop()
            logger.info(f"Stopped {domain} domain master")
        
        # Stop listener thread
        if self.completion_listener_thread:
            self.completion_listener_thread.join(timeout=1.0)
        
        # Unsubscribe
        self.pubsub.unsubscribe()
        
        logger.info("GlobalMaster stopped")
    
    def create_workflow(self, query: str, context: List[str] = None) -> str:
        """
        Create a new workflow for processing a natural language query.
        
        Args:
            query: The natural language query
            context: Optional context information
            
        Returns:
            Workflow request ID
        """
        request_id = str(uuid.uuid4())
        
        # Create workflow structure
        workflow = {
            "request_id": request_id,
            "created_at": time.time(),
            "data": {
                "query": query,
                "context": context or []
            },
            "steps": [
                {"domain": "nlp", "status": "pending", "start_time": None, "end_time": None},
                {"domain": "query", "status": "pending", "start_time": None, "end_time": None},
                {"domain": "response", "status": "pending", "start_time": None, "end_time": None}
            ],
            "current_domain": "nlp"
        }
        
        # Store workflow in Redis
        workflow_key = f"workflow:{request_id}"
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)  # 1 hour expiration
        
        # Add to active workflows
        self.active_workflows[request_id] = {
            "status": "created",
            "query": query,
            "created_at": time.time()
        }
        
        # Update metrics
        self.request_counter.labels(status="created").inc()
        self.active_workflows_gauge.inc()
        
        # Log workflow creation
        log_workflow_start(request_id, query)
        
        logger.info(f"Created workflow {request_id} for query: {query}")
        return request_id
    
    def start_workflow(self, request_id: str):
        """
        Start processing a workflow.
        
        Args:
            request_id: Workflow request ID
        """
        # Get workflow from Redis
        workflow_key = f"workflow:{request_id}"
        workflow_json = self.redis.get(workflow_key)
        
        if not workflow_json:
            logger.error(f"Workflow {request_id} not found")
            return False
        
        # Parse workflow
        workflow = json.loads(workflow_json)
        
        # Update status
        if request_id in self.active_workflows:
            self.active_workflows[request_id]["status"] = "processing"
        
        # Update current step status and record start time
        for step in workflow["steps"]:
            if step["domain"] == "nlp":
                step["status"] = "processing"
                step["start_time"] = time.time()
                break
        
        # Update workflow in Redis
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
        
        # Send to NLP domain (always starts there)
        self.redis.publish("domain:nlp:requests", json.dumps(workflow))
        
        # Update metrics
        self.request_counter.labels(status="started").inc()
        
        logger.info(f"Started workflow {request_id}")
        return True
    
    def get_workflow_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a workflow.
        
        Args:
            request_id: Workflow request ID
            
        Returns:
            Workflow status information or None if not found
        """
        # Get workflow from Redis
        workflow_key = f"workflow:{request_id}"
        workflow_json = self.redis.get(workflow_key)
        
        if not workflow_json:
            return None
        
        # Parse workflow
        workflow = json.loads(workflow_json)
        
        # Extract status information
        status_info = {
            "request_id": request_id,
            "created_at": workflow.get("created_at"),
            "current_domain": workflow.get("current_domain"),
            "completed": "completed_at" in workflow,
            "has_error": "error" in workflow,
        }
        
        if "completed_at" in workflow:
            status_info["completed_at"] = workflow["completed_at"]
            status_info["processing_time"] = workflow["completed_at"] - workflow.get("created_at", 0)
        
        if "error" in workflow:
            status_info["error"] = workflow["error"]
            status_info["error_details"] = workflow.get("error_details")
        
        # Add domain steps status
        status_info["steps"] = workflow.get("steps", [])
        
        # Get metrics for this workflow
        workflow_metrics = metrics_logger.get_workflow_metrics(request_id)
        if workflow_metrics:
            status_info["metrics"] = workflow_metrics
        
        return status_info
    
    def get_workflow_result(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the final result of a completed workflow.
        
        Args:
            request_id: Workflow request ID
            
        Returns:
            Workflow result data or None if not found/completed
        """
        # Get workflow from Redis
        workflow_key = f"workflow:{request_id}"
        workflow_json = self.redis.get(workflow_key)
        
        if not workflow_json:
            return None
        
        # Parse workflow
        workflow = json.loads(workflow_json)
        
        # Check if completed
        if "completed_at" not in workflow:
            logger.warning(f"Workflow {request_id} not completed yet")
            return {
                "completed": False,
                "request_id": request_id
            }
        
        # Extract result information
        result = {
            "completed": True,
            "request_id": request_id,
            "created_at": workflow.get("created_at"),
            "completed_at": workflow.get("completed_at"),
            "processing_time": workflow.get("completed_at", 0) - workflow.get("created_at", 0),
            "original_query": workflow.get("data", {}).get("query", ""),
            "response": workflow.get("data", {}).get("response", ""),
        }
        
        # Add error information if present
        if "error" in workflow:
            result["error"] = workflow["error"]
            result["success"] = False
        else:
            result["success"] = True
        
        # Add SPARQL query if available
        sparql_query = workflow.get("data", {}).get("sparql_query")
        if sparql_query:
            result["sparql_query"] = sparql_query
        
        # Include domain processing times
        domain_times = {}
        for step in workflow.get("steps", []):
            domain = step.get("domain")
            start_time = step.get("start_time")
            end_time = step.get("end_time")
            
            if domain and start_time and end_time:
                domain_times[domain] = end_time - start_time
                
        if domain_times:
            result["domain_processing_times"] = domain_times
            
        return result
    
    def _listen_for_messages(self):
        """Listen for workflow completions and domain transitions."""
        while self.running:
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    if channel == "global:completions":
                        workflow = json.loads(message["data"])
                        self._handle_workflow_completion(workflow)
                    elif channel == "global:domain_transitions":
                        transition_data = json.loads(message["data"])
                        self._handle_domain_transition(transition_data)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
    
    def _handle_workflow_completion(self, workflow: Dict[str, Any]):
        """
        Handle a completed workflow.
        
        Args:
            workflow: The completed workflow data
        """
        request_id = workflow.get("request_id")
        
        # Calculate processing time
        created_at = workflow.get("created_at", 0)
        completed_at = time.time()
        processing_time = completed_at - created_at
        
        # Add completion timestamp if not already present
        if "completed_at" not in workflow:
            workflow["completed_at"] = completed_at
            
        # Update the final step's end time
        for step in workflow.get("steps", []):
            if step["domain"] == "response":
                step["status"] = "completed"
                step["end_time"] = completed_at
        
        # Update workflow in Redis
        workflow_key = f"workflow:{request_id}"
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)  # 1 hour expiration
        
        # Update metrics
        self.processing_time.observe(processing_time)
        status = "error" if "error" in workflow else "completed"
        self.request_counter.labels(status=status).inc()
        
        # Log workflow completion
        success = "error" not in workflow
        log_workflow_completion(request_id, success, processing_time)
        
        if request_id in self.active_workflows:
            # Remove from active workflows
            del self.active_workflows[request_id]
            self.active_workflows_gauge.dec()
        
        logger.info(f"Workflow {request_id} completed in {processing_time:.2f}s with status: {status}")
    
    def _handle_domain_transition(self, transition_data: Dict[str, Any]):
        """
        Handle a domain transition.
        
        Args:
            transition_data: Data about the domain transition
        """
        request_id = transition_data.get("request_id")
        from_domain = transition_data.get("from_domain")
        to_domain = transition_data.get("to_domain")
        
        if not all([request_id, from_domain, to_domain]):
            logger.error(f"Invalid domain transition data: {transition_data}")
            return
            
        # Get workflow from Redis
        workflow_key = f"workflow:{request_id}"
        workflow_json = self.redis.get(workflow_key)
        
        if not workflow_json:
            logger.error(f"Workflow {request_id} not found during domain transition")
            return
            
        # Parse workflow
        workflow = json.loads(workflow_json)
        
        # Record transition time
        transition_time = time.time()
        
        # Update step status for the domain we're leaving
        for step in workflow.get("steps", []):
            if step["domain"] == from_domain:
                step["status"] = "completed"
                step["end_time"] = transition_time
                
                # Calculate domain processing time
                if step["start_time"]:
                    domain_time = transition_time - step["start_time"]
                    log_domain_processing(request_id, from_domain, domain_time)
                    
            elif step["domain"] == to_domain:
                step["status"] = "processing"
                step["start_time"] = transition_time
                
        # Update current domain
        workflow["current_domain"] = to_domain
        
        # Update workflow in Redis
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
        
        logger.info(f"Workflow {request_id} transitioned from {from_domain} to {to_domain}")
    
    def get_health(self) -> Dict[str, Any]:
        """
        Get the health status of the global master and domain masters.
        
        Returns:
            Health status information
        """
        # Get domain masters health
        domain_health = {}
        for domain, master in self.domain_masters.items():
            domain_health[domain] = master.get_health()
        
        # Determine overall health
        all_domains_healthy = all(
            health.get("status") == "healthy" 
            for health in domain_health.values()
        )
        
        return {
            "status": "healthy" if (self.running and all_domains_healthy) else "unhealthy",
            "active": self.running,
            "active_workflows": len(self.active_workflows),
            "domain_masters": domain_health
        }