import hashlib
import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import redis
from prometheus_client import Counter, Gauge, Histogram

from database.elastic_client import ElasticClient
from database.ontology_store import OntologyStore
from database.qdrant_client import QdrantClient
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
    - Caching query results to avoid redundant processing
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
        
        # Cache configuration
        self.query_cache_expiry = 86400  # 24 hours for query cache
        self.sparql_cache_expiry = 86400  # 24 hours for SPARQL results cache
        
        # Create ontology store for use with domain masters
        ontology_store = OntologyStore(
            endpoint_url=endpoint_url,
            redis_url=redis_url
        )
        
        # Create Qdrant client for entity recognition
        qdrant_client = QdrantClient()
        
        # Initialize domain masters
        self.domain_masters = {
            "nlp": NLPDomainMaster(redis_url, ontology_store, qdrant_client),
            "query": QueryDomainMaster(redis_url, ontology_store),
            "response": ResponseDomainMaster(redis_url, endpoint_url)
        }
        
        # Initialize Elasticsearch client for query caching
        self.elastic_client = ElasticClient()
        
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
        
        # Cache metrics
        self.query_cache_hits = Counter('query_cache_hits_total', 'Total query cache hits')
        self.query_cache_misses = Counter('query_cache_misses_total', 'Total query cache misses')
        self.sparql_cache_hits = Counter('sparql_cache_hits_total', 'Total SPARQL cache hits')
        self.sparql_cache_misses = Counter('sparql_cache_misses_total', 'Total SPARQL cache misses')
        
        logger.info("GlobalMaster initialized")
        
    # Cache-related methods
    def _generate_query_cache_key(self, query: str, context: List[str] = None) -> str:
        """
        Generate a cache key for a natural language query.
        
        Args:
            query: The natural language query
            context: Optional context information
            
        Returns:
            Cache key string
        """
        # Normalize query by lowercasing and removing extra whitespace
        normalized_query = query.lower().strip()
        
        # Create a composite key from the query and context
        key_content = normalized_query
        if context:
            # Sort context items to ensure consistent keys regardless of order
            key_content += "||" + "||".join(sorted([str(item) for item in context]))
            
        # Create a hash of the content
        query_hash = hashlib.md5(key_content.encode('utf-8')).hexdigest()
        return f"cache:query:{query_hash}"
        
    def _generate_sparql_cache_key(self, sparql_query: str) -> str:
        """
        Generate a cache key for a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query
            
        Returns:
            Cache key string
        """
        # Normalize query by removing extra whitespace
        normalized_query = " ".join(sparql_query.split())
        
        # Create a hash of the query
        query_hash = hashlib.md5(normalized_query.encode('utf-8')).hexdigest()
        return f"cache:sparql:{query_hash}"
        
    def get_cached_query_result(self, query: str, context: List[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get cached result for a natural language query.
        
        Args:
            query: The natural language query
            context: Optional context information
            
        Returns:
            Cached result or None if not found
        """
        try:
            cache_key = self._generate_query_cache_key(query, context)
            cached_data = self.redis.get(cache_key)
            
            if cached_data:
                logger.info(f"Query cache hit for: {query[:50]}...")
                self.query_cache_hits.inc()
                
                result = json.loads(cached_data)
                result["from_cache"] = True
                result["cache_type"] = "query"
                return result
            else:
                self.query_cache_misses.inc()
                logger.debug(f"Query cache miss for: {query[:50]}...")
        except Exception as e:
            logger.warning(f"Error retrieving from query cache: {e}")
            
        return None
        
    def cache_query_result(self, query: str, result: Dict[str, Any], context: List[str] = None) -> None:
        """
        Cache the result for a natural language query.
        
        Args:
            query: The natural language query
            result: The result to cache
            context: Optional context information
        """
        try:
            # Don't cache errors
            if result.get("error"):
                return
                
            cache_key = self._generate_query_cache_key(query, context)
            
            # Create a copy of the result without cache metadata
            cache_result = result.copy()
            cache_result.pop("from_cache", None)
            cache_result.pop("cache_type", None)
            
            self.redis.setex(
                cache_key,
                self.query_cache_expiry,
                json.dumps(cache_result)
            )
            logger.debug(f"Cached query result for: {query[:50]}...")
        except Exception as e:
            logger.warning(f"Error caching query result: {e}")
            
    def get_cached_sparql_result(self, sparql_query: str) -> Optional[Dict[str, Any]]:
        """
        Get cached result for a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query
            
        Returns:
            Cached result or None if not found
        """
        try:
            cache_key = self._generate_sparql_cache_key(sparql_query)
            cached_data = self.redis.get(cache_key)
            
            if cached_data:
                logger.info(f"SPARQL cache hit for: {sparql_query[:50]}...")
                self.sparql_cache_hits.inc()
                
                result = json.loads(cached_data)
                result["from_cache"] = True
                result["cache_type"] = "sparql"
                return result
            else:
                self.sparql_cache_misses.inc()
                logger.debug(f"SPARQL cache miss for: {sparql_query[:50]}...")
        except Exception as e:
            logger.warning(f"Error retrieving from SPARQL cache: {e}")
            
        return None
        
    def cache_sparql_result(self, sparql_query: str, result: Dict[str, Any]) -> None:
        """
        Cache the result for a SPARQL query.
        
        Args:
            sparql_query: The SPARQL query
            result: The result to cache
        """
        try:
            # Don't cache errors
            if result.get("error"):
                return
                
            cache_key = self._generate_sparql_cache_key(sparql_query)
            
            # Create a copy of the result without cache metadata
            cache_result = result.copy()
            cache_result.pop("from_cache", None)
            cache_result.pop("cache_type", None)
            
            self.redis.setex(
                cache_key,
                self.sparql_cache_expiry,
                json.dumps(cache_result)
            )
            logger.debug(f"Cached SPARQL result for: {sparql_query[:50]}...")
        except Exception as e:
            logger.warning(f"Error caching SPARQL result: {e}")
            
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        query_cache_keys = len(self.redis.keys("cache:query:*"))
        sparql_cache_keys = len(self.redis.keys("cache:sparql:*"))
        
        query_hit_count = self.query_cache_hits._value.get()
        query_miss_count = self.query_cache_misses._value.get()
        query_total = query_hit_count + query_miss_count
        
        sparql_hit_count = self.sparql_cache_hits._value.get()
        sparql_miss_count = self.sparql_cache_misses._value.get()
        sparql_total = sparql_hit_count + sparql_miss_count
        
        return {
            "query_cache": {
                "entries": query_cache_keys,
                "hits": query_hit_count,
                "misses": query_miss_count,
                "hit_rate": (query_hit_count / query_total * 100) if query_total > 0 else 0,
                "expiry": self.query_cache_expiry
            },
            "sparql_cache": {
                "entries": sparql_cache_keys,
                "hits": sparql_hit_count,
                "misses": sparql_miss_count,
                "hit_rate": (sparql_hit_count / sparql_total * 100) if sparql_total > 0 else 0,
                "expiry": self.sparql_cache_expiry
            }
        }
        
    def clear_cache(self, cache_type: str = "all") -> Dict[str, Any]:
        """
        Clear the cache.
        
        Args:
            cache_type: Type of cache to clear ("all", "query", or "sparql")
            
        Returns:
            Dictionary with number of entries cleared
        """
        query_cleared = 0
        sparql_cleared = 0
        
        if cache_type in ["all", "query"]:
            query_keys = self.redis.keys("cache:query:*")
            if query_keys:
                query_cleared = len(query_keys)
                self.redis.delete(*query_keys)
                logger.info(f"Cleared {query_cleared} query cache entries")
                
        if cache_type in ["all", "sparql"]:
            sparql_keys = self.redis.keys("cache:sparql:*")
            if sparql_keys:
                sparql_cleared = len(sparql_keys)
                self.redis.delete(*sparql_keys)
                logger.info(f"Cleared {sparql_cleared} SPARQL cache entries")
                
        return {
            "query_entries_cleared": query_cleared,
            "sparql_entries_cleared": sparql_cleared,
            "total_cleared": query_cleared + sparql_cleared
        }
    
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
            if (message and message["type"] == "message"):
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
        
        if not request_id or request_id not in self.active_workflows:
            logger.warning(f"Completion for unknown workflow {request_id}")
            return
        
        # Log completion
        workflow_data = workflow.get("data", {})
        error = workflow.get("error")
        
        if error:
            logger.error(f"Workflow {request_id} completed with error: {error}")
            self.request_counter.labels(status="error").inc()
            log_workflow_completion(workflow, success=False)
        else:
            logger.info(f"Workflow {request_id} completed successfully")
            self.request_counter.labels(status="success").inc()
            log_workflow_completion(workflow, success=True)
            
            # Cache successful workflow results if not already from cache
            if not workflow_data.get("from_cache", False):
                try:
                    # Cache the query result for future use
                    query = workflow_data.get("query")
                    context = workflow_data.get("context", [])
                    sparql_query = workflow_data.get("sparql_query")
                    query_results = workflow_data.get("query_results", {})
                    response = workflow_data.get("response", "")
                    
                    if query and sparql_query and response:
                        # Create a result object with all necessary data
                        result = {
                            "sparql_query": sparql_query,
                            "response": response,
                            "query_results": query_results
                        }
                        
                        # Cache natural language query to result mapping
                        self.cache_query_result(query, result, context)
                        
                        # If we have a valid SPARQL query and results, also cache those
                        if sparql_query and query_results:
                            self.cache_sparql_result(sparql_query, {
                                "success": True,
                                "results": query_results
                            })
                            
                        logger.info(f"Cached results for workflow {request_id}")
                except Exception as e:
                    logger.warning(f"Error caching workflow results: {e}")
        
        # Remove from active workflows
        self.active_workflows.pop(request_id, None)
        self.active_workflows_gauge.dec()
        
        # Calculate and record processing time
        if "created_at" in workflow and "completed_at" in workflow:
            processing_time = workflow["completed_at"] - workflow["created_at"]
            self.processing_time.observe(processing_time)
            logger.debug(f"Workflow {request_id} completed in {processing_time:.2f}s")
        
        # Record workflow in history (limited size)
        self.workflow_history.append(workflow)
        if len(self.workflow_history) > self.max_history_size:
            self.workflow_history.pop(0)
    
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

    def process_query(self, query: str, context: List[str] = None) -> Dict[str, Any]:
        """
        Process a natural language query through the NL2SPARQL pipeline.
        
        Args:
            query: The natural language query
            context: Optional context information
            
        Returns:
            Result dictionary
        """
        # Check Redis cache first
        cached_result = self.get_cached_query_result(query, context)
        if cached_result:
            return cached_result
            
        # If not found in Redis, check Elasticsearch for similar queries
        similar_query = self.elastic_client.search_similar_query(query, min_score=0.85)
        if similar_query:
            logger.info(f"Found similar query in ElasticSearch: {similar_query['natural_query'][:50]}...")
            
            # Return the similar query result but mark it as from Elasticsearch
            result = {
                "sparql_query": similar_query["sparql_query"],
                "response": similar_query["response"],
                "from_cache": True,
                "cache_type": "elasticsearch",
                "similarity_score": similar_query["score"],
                "original_query": similar_query["natural_query"]
            }
            
            # Also add to Redis cache for faster retrieval next time
            self.cache_query_result(query, {
                "sparql_query": similar_query["sparql_query"],
                "response": similar_query["response"]
            }, context)
            
            return result
        
        # No cache hits, process the query
        logger.info(f"Processing new query: {query[:50]}...")
        
        # Create and start workflow
        request_id = self.create_workflow(query, context)
        self.start_workflow(request_id)
        
        # Wait for workflow to complete with timeout
        start_time = time.time()
        max_wait_time = 60.0  # Maximum wait time in seconds
        
        while time.time() - start_time < max_wait_time:
            # Check if workflow is complete
            workflow_result = self.get_workflow_result(request_id)
            
            if workflow_result and workflow_result.get("completed"):
                # If successful, store in Elasticsearch for future similarity matching
                if workflow_result.get("success") and "sparql_query" in workflow_result:
                    self.elastic_client.store_query_sparql_pair({
                        "natural_query": query,
                        "sparql_query": workflow_result["sparql_query"],
                        "response": workflow_result.get("response", ""),
                        "context": context or [],
                        "timestamp": time.time(),
                        "execution_time": workflow_result.get("processing_time", 0),
                        "successful": workflow_result.get("success", True)
                    })
                    logger.info(f"Stored query-SPARQL pair in Elasticsearch for: {query[:50]}...")
                
                return workflow_result
                
            # Small sleep to prevent CPU spinning
            time.sleep(0.1)
            
        # Timeout reached
        logger.error(f"Workflow {request_id} timed out")
        return {
            "error": "Request timed out",
            "request_id": request_id
        }