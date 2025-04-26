import json
import uuid
import time
import threading
from typing import Dict, Any, List, Optional

import redis
from prometheus_client import Counter, Histogram, Gauge

from utils.logging_utils import setup_logging
from utils.circuit_breaker import CircuitBreaker

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class GlobalMaster:
    """
    Global master responsible for coordinating the entire NL2SPARQL pipeline.
    - Manages workflow creation and tracking
    - Dispatches workflows to domain masters
    - Collects results from domain masters
    - Provides status updates and results to clients
    """
    
    def __init__(self, redis_url: str):
        """
        Initialize the global master.
        
        Args:
            redis_url: Redis connection URL
        """
        self.redis_url = redis_url
        self.redis = redis.from_url(redis_url)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        
        # Track active workflows
        self.active_workflows = {}
        
        # Thread control
        self.running = False
        self.completion_listener_thread = None
        
        # Circuit breakers for domain masters
        self.domain_circuit_breakers = {
            domain: CircuitBreaker(
                name=f"{domain}_domain_master",
                failure_threshold=5,
                recovery_timeout=30,
                callback=lambda d=domain: logger.error(f"Circuit breaker tripped for {d} domain")
            )
            for domain in ["nlp", "query", "response"]
        }
        
        # Prometheus metrics
        self.request_counter = Counter(
            'global_requests_total', 
            'Total NL2SPARQL requests received',
            ['status']
        )
        self.processing_time = Histogram(
            'global_processing_seconds', 
            'Total end-to-end processing time for requests'
        )
        self.active_workflows_gauge = Gauge(
            'global_active_workflows',
            'Number of currently active workflows'
        )
        self.domain_timing = Histogram(
            'domain_processing_seconds',
            'Time spent in each domain',
            ['domain']
        )
        
        logger.info("GlobalMaster initialized")
    
    def start(self):
        """Start the global master services."""
        if self.running:
            return
            
        # Subscribe to completion channel from domain masters
        self.pubsub.subscribe("global:completions")
        
        # Start listener thread for completions
        self.running = True
        self.completion_listener_thread = threading.Thread(target=self._listen_for_completions)
        self.completion_listener_thread.daemon = True
        self.completion_listener_thread.start()
        
        logger.info("GlobalMaster started")
    
    def stop(self):
        """Stop the global master services."""
        self.running = False
        if self.completion_listener_thread:
            self.completion_listener_thread.join(timeout=1.0)
        self.pubsub.unsubscribe()
        logger.info("GlobalMaster stopped")
    
    def process_nl_query(self, query: str, context: List[Dict[str, Any]] = None) -> str:
        """
        Process a natural language query through the domain masters.
        
        Args:
            query: The natural language query
            context: Optional context for the query
            
        Returns:
            Request ID for tracking the workflow
        """
        context = context or []
        request_id = str(uuid.uuid4())
        
        # Create a new workflow
        workflow = {
            "request_id": request_id,
            "created_at": time.time(),
            "data": {
                "query": query,
                "context": context
            },
            "steps": [
                {"domain": "nlp", "status": "pending"},
                {"domain": "query", "status": "pending"},
                {"domain": "response", "status": "pending"}
            ],
            "current_domain": "nlp"
        }
        
        # Store workflow in Redis
        workflow_key = f"workflow:{request_id}"
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)  # Expire after 1 hour
        
        # Add to active workflows
        self.active_workflows[request_id] = {
            "status": "processing",
            "started_at": time.time()
        }
        self.active_workflows_gauge.inc()
        
        # Update metrics
        self.request_counter.labels(status="received").inc()
        
        # Mark NLP domain step as started
        workflow["steps"][0]["started_at"] = time.time()
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
        
        # Dispatch to NLP domain
        try:
            with self.domain_circuit_breakers["nlp"]:
                self.redis.publish("domain:nlp:requests", json.dumps(workflow))
                logger.info(f"GlobalMaster dispatched workflow {request_id} to NLP domain")
        except Exception as e:
            logger.error(f"Error dispatching to NLP domain: {e}")
            workflow["steps"][0]["status"] = "error"
            workflow["steps"][0]["error"] = str(e)
            workflow["error"] = f"Failed to dispatch to NLP domain: {str(e)}"
            self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
            self.request_counter.labels(status="error").inc()
        
        return request_id
    
    def _listen_for_completions(self):
        """Listen for workflow completions from domain masters."""
        while self.running:
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    workflow = json.loads(message["data"])
                    request_id = workflow.get("request_id")
                    
                    if not request_id:
                        logger.warning("GlobalMaster received completion without request_id")
                        continue
                        
                    # Process the completed workflow
                    self._process_completed_workflow(workflow)
                    
                except Exception as e:
                    logger.error(f"Error processing completion in GlobalMaster: {e}")
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
    
    def _process_completed_workflow(self, workflow: Dict[str, Any]):
        """
        Process a completed workflow.
        
        Args:
            workflow: The completed workflow data
        """
        request_id = workflow.get("request_id")
        
        # Update workflow in Redis
        workflow["completed_at"] = time.time()
        workflow["status"] = "completed"
        
        # Calculate domain timing metrics if timestamps are available
        for i, step in enumerate(workflow.get("steps", [])):
            if step.get("started_at") and step.get("completed_at"):
                domain = step.get("domain")
                processing_time = step.get("completed_at") - step.get("started_at")
                self.domain_timing.labels(domain=domain).observe(processing_time)
        
        # Remove from active workflows
        if request_id in self.active_workflows:
            started_at = self.active_workflows[request_id].get("started_at")
            if started_at:
                self.processing_time.observe(time.time() - started_at)
            
            del self.active_workflows[request_id]
            self.active_workflows_gauge.dec()
        
        # Check for errors
        has_error = "error" in workflow or any(
            "error" in step for step in workflow.get("steps", [])
        )
        
        # Update metrics
        if has_error:
            self.request_counter.labels(status="error").inc()
        else:
            self.request_counter.labels(status="success").inc()
        
        # Store final workflow state in Redis
        self.redis.set(f"workflow:{request_id}", json.dumps(workflow), ex=3600)
        
        logger.info(f"GlobalMaster processed completion for workflow {request_id}")
    
    def get_workflow_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current status of a workflow.
        
        Args:
            request_id: The workflow request ID
            
        Returns:
            Current workflow status or None if not found
        """
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if not workflow_json:
            return None
        
        try:
            workflow = json.loads(workflow_json)
            
            # Calculate completion percentage
            total_steps = len(workflow.get("steps", []))
            completed_steps = sum(
                1 for step in workflow.get("steps", [])
                if step.get("status") == "completed"
            )
            
            completion_percentage = (completed_steps / max(1, total_steps)) * 100
            
            return {
                "request_id": request_id,
                "status": workflow.get("status", "processing"),
                "current_domain": workflow.get("current_domain"),
                "completion_percentage": completion_percentage,
                "completed_steps": completed_steps,
                "total_steps": total_steps,
                "errors": workflow.get("errors", []),
                "created_at": workflow.get("created_at"),
                "completed_at": workflow.get("completed_at")
            }
        except json.JSONDecodeError:
            return None
    
    def get_workflow_result(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the result of a completed workflow.
        
        Args:
            request_id: The workflow request ID
            
        Returns:
            Workflow result or None if not found or not completed
        """
        workflow_json = self.redis.get(f"workflow:{request_id}")
        if not workflow_json:
            return None
        
        try:
            workflow = json.loads(workflow_json)
            
            # Check if workflow is completed
            all_completed = all(
                step.get("status") == "completed"
                for step in workflow.get("steps", [])
            )
            
            if not all_completed and not workflow.get("error"):
                return {
                    "request_id": request_id,
                    "status": "processing",
                    "message": "Workflow still processing"
                }
            
            # Get the final response if available
            data = workflow.get("data", {})
            
            result = {
                "request_id": request_id,
                "original_query": data.get("query", ""),
                "refined_query": data.get("refined_query", ""),
                "response": data.get("response", ""),
                "status": "completed" if not workflow.get("error") else "error"
            }
            
            # Include error if present
            if workflow.get("error"):
                result["error"] = workflow.get("error")
            
            # Include SPARQL query and other data for debugging
            result["debug_info"] = {
                "sparql_query": data.get("sparql_query", ""),
                "entities": data.get("entities", {}),
                "validation_result": data.get("validation_result", {})
            }
            
            return result
        except json.JSONDecodeError:
            return None
    
    def get_health(self) -> Dict[str, Any]:
        """
        Get the health status of the global master and domain masters.
        
        Returns:
            Health status information
        """
        return {
            "global_master": {
                "status": "healthy" if self.running else "unhealthy",
                "active_workflows": len(self.active_workflows)
            },
            "domain_masters": {
                domain: {
                    "status": "healthy" if not cb.is_open() else "unhealthy",
                    "circuit_breaker": "closed" if not cb.is_open() else "open"
                }
                for domain, cb in self.domain_circuit_breakers.items()
            },
            "redis": {
                "status": "healthy" if self._check_redis() else "unhealthy"
            }
        }
    
    def _check_redis(self) -> bool:
        """Check if Redis connection is healthy."""
        try:
            return self.redis.ping()
        except Exception:
            return False