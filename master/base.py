import json
import threading
import time
from typing import Any, Dict, Optional

import redis
from prometheus_client import Counter, Gauge, Histogram

from database.qdrant_client import QdrantClient
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class DomainMaster:
    """
    Base class for domain masters in the NL2SPARQL system.
    
    Domain masters are responsible for:
    - Coordinating slaves within their domain
    - Managing workflow transitions through the domain
    - Communicating results back to the global master
    """
    
    def __init__(self, domain: str, redis_url: str, qdrant_client: QdrantClient = None):
        """
        Initialize the domain master.
        
        Args:
            domain: Domain name (nlp, query, response)
            redis_url: Redis connection URL
        """
        self.domain = domain
        self.redis_url = redis_url
        self.redis = redis.from_url(redis_url)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        
        # Track active workflows within this domain
        self.active_workflows = {}
        
        # Thread control
        self.running = False
        self.request_listener_thread = None
        self.result_listener_thread = None
        
        # Prometheus metrics
        self.workflow_counter = Counter(
            f'{domain}_workflows_total',
            f'Total workflows processed by {domain} domain',
            ['status']
        )
        self.processing_time = Histogram(
            f'{domain}_processing_seconds',
            f'Time spent processing workflows in {domain} domain'
        )
        self.active_workflows_gauge = Gauge(
            f'{domain}_active_workflows',
            f'Number of active workflows in {domain} domain'
        )
        self.task_counter = Counter(
            f'{domain}_tasks_total',
            f'Total tasks dispatched by {domain} domain',
            ['slave_type', 'status']
        )
        
        logger.info(f"{domain} DomainMaster initialized")
    
    def start(self):
        """Start the domain master services."""
        if self.running:
            return
            
        # Subscribe to request and result channels
        self.pubsub.subscribe(f"domain:{self.domain}:requests")
        self.pubsub.subscribe(f"domain:{self.domain}:results")
        
        # Start listener threads
        self.running = True
        
        # Thread for listening to incoming workflow requests
        self.request_listener_thread = threading.Thread(target=self._listen_for_requests)
        self.request_listener_thread.daemon = True
        self.request_listener_thread.start()
        
        # Thread for listening to slave results
        self.result_listener_thread = threading.Thread(target=self._listen_for_results)
        self.result_listener_thread.daemon = True
        self.result_listener_thread.start()
        
        logger.info(f"{self.domain} DomainMaster started")
    
    def stop(self):
        """Stop the domain master services."""
        self.running = False
        if self.request_listener_thread:
            self.request_listener_thread.join(timeout=1.0)
        if self.result_listener_thread:
            self.result_listener_thread.join(timeout=1.0)
        self.pubsub.unsubscribe()
        logger.info(f"{self.domain} DomainMaster stopped")
    
    def _listen_for_requests(self):
        """Listen for incoming workflow requests."""
        while self.running:
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    # Process only messages from our request channel
                    if channel == f"domain:{self.domain}:requests":
                        workflow = json.loads(message["data"])
                        self._handle_workflow_request(workflow)
                        
                except Exception as e:
                    logger.error(f"Error processing request in {self.domain} DomainMaster: {e}")
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
    
    def _listen_for_results(self):
        """Listen for results from slaves."""
        while self.running:
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode('utf-8')
                    
                    # Process only messages from our result channel
                    if channel == f"domain:{self.domain}:results":
                        result = json.loads(message["data"])
                        self._handle_slave_result(result)
                        
                except Exception as e:
                    logger.error(f"Error processing result in {self.domain} DomainMaster: {e}")
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
    
    def _handle_workflow_request(self, workflow: Dict[str, Any]):
        """
        Handle an incoming workflow request.
        
        Args:
            workflow: The workflow data
        """
        request_id = workflow.get("request_id")
        
        logger.info(f"{self.domain} DomainMaster received workflow {request_id}")
        
        # Mark this domain's step as started
        for i, step in enumerate(workflow.get("steps", [])):
            if step.get("domain") == self.domain:
                workflow["steps"][i]["status"] = "processing"
                workflow["steps"][i]["started_at"] = time.time()
                break
                
        # Update workflow in Redis
        workflow_key = f"workflow:{request_id}"
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
        
        # Update metrics
        self.workflow_counter.labels(status="received").inc()
        self.active_workflows_gauge.inc()
        
        # Process the workflow
        self.process_workflow(workflow)
    
    def _handle_slave_result(self, result: Dict[str, Any]):
        """
        Handle a result from a slave.
        
        Args:
            result: The result data
        """
        request_id = result.get("request_id")
        success = result.get("success", False)
        
        # Update task metrics
        slave_type = result.get("slave_type", "unknown")
        status = "success" if success else "error"
        self.task_counter.labels(slave_type=slave_type, status=status).inc()
        
        logger.info(f"{self.domain} DomainMaster received result for task {result.get('task_id')} (success={success})")
        
        # Process the result through the domain-specific logic
        self.process_slave_result(result)
    
    def process_workflow(self, workflow: Dict[str, Any]):
        """
        Process an incoming workflow. To be implemented by subclasses.
        
        Args:
            workflow: The workflow data
        """
        raise NotImplementedError("Subclasses must implement process_workflow")
    
    def process_slave_result(self, result: Dict[str, Any]):
        """
        Process results from slave tasks. To be implemented by subclasses.
        
        Args:
            result: The result data from a slave
        """
        raise NotImplementedError("Subclasses must implement process_slave_result")
    
    def _dispatch_to_slave_pool(self, slave_type: str, task: Dict[str, Any]):
        """
        Dispatch a task to a slave pool.
        
        Args:
            slave_type: The type of slave to dispatch to
            task: The task to dispatch
        """
        # Publish task to the appropriate slave pool
        channel = f"slave_pool:{self.domain}:{slave_type}:tasks"
        self.redis.publish(channel, json.dumps(task))
        
        logger.debug(f"{self.domain} DomainMaster dispatched task {task.get('task_id')} to {slave_type} pool")
    
    def _complete_workflow(self, workflow: Dict[str, Any], next_domain: Optional[str] = None):
        """
        Complete a workflow in this domain and optionally forward to the next domain.
        
        Args:
            workflow: The workflow data
            next_domain: Optional next domain to forward the workflow to
        """
        request_id = workflow.get("request_id")
        
        # Mark this domain's step as completed
        for i, step in enumerate(workflow.get("steps", [])):
            if step.get("domain") == self.domain:
                step["status"] = "completed"
                step["completed_at"] = time.time()
                
                # Calculate and record processing time
                if step.get("started_at"):
                    self.processing_time.observe(step["completed_at"] - step["started_at"])
                break
                
        # Update current domain in workflow
        if next_domain:
            workflow["current_domain"] = next_domain
            
            # Mark the next domain's step as started
            for i, step in enumerate(workflow.get("steps", [])):
                if step.get("domain") == next_domain:
                    workflow["steps"][i]["status"] = "processing"
                    workflow["steps"][i]["started_at"] = time.time()
                    break
        
        # Update workflow in Redis
        workflow_key = f"workflow:{request_id}"
        self.redis.set(workflow_key, json.dumps(workflow), ex=3600)
        
        # Remove from active workflows
        if request_id in self.active_workflows:
            del self.active_workflows[request_id]
            self.active_workflows_gauge.dec()
        
        # Update metrics
        self.workflow_counter.labels(status="completed").inc()
        
        logger.info(f"{self.domain} DomainMaster completed workflow {request_id}")
        
        # Forward to next domain or complete globally
        if next_domain:
            self.redis.publish(f"domain:{next_domain}:requests", json.dumps(workflow))
            logger.info(f"{self.domain} DomainMaster forwarded workflow {request_id} to {next_domain}")
        else:
            # Publish to global completion channel
            self.redis.publish("global:completions", json.dumps(workflow))
            logger.info(f"{self.domain} DomainMaster published global completion for workflow {request_id}")
    
    def get_health(self) -> Dict[str, Any]:
        """
        Get the health status of this domain master.
        
        Returns:
            Health status information
        """
        return {
            "domain": self.domain,
            "status": "healthy" if self.running else "unhealthy",
            "active_workflows": len(self.active_workflows)
        }