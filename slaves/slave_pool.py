import importlib
import json
import random
import threading
import time
from typing import Any, Dict, List, Optional, Type

import redis
from prometheus_client import Counter, Gauge

from slaves.base import AbstractSlave
from utils.load_balancer import LoadBalancer
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class SlavePool:
    """
    Manages a pool of slave instances of the same type.
    Supports load balancing, automatic scaling, and fault tolerance.
    """
    
    def __init__(
        self,
        domain: str,
        slave_type: str,
        slave_class_path: str,
        redis_url: str,
        initial_size: int = 2,
        max_size: int = 10,
        slave_config: Dict[str, Any] = None
    ):
        """
        Initialize the slave pool.
        
        Args:
            domain: Domain name (nlp, query, response)
            slave_type: Type of slave (e.g., query_refinement, ontology_mapping)
            slave_class_path: Import path to the slave class
            redis_url: Redis connection URL
            initial_size: Initial number of slaves to create
            max_size: Maximum number of slaves allowed in this pool
            slave_config: Configuration to pass to each slave instance
        """
        self.domain = domain
        self.slave_type = slave_type
        self.redis_url = redis_url
        self.slave_class_path = slave_class_path
        self.max_size = max_size
        self.slave_config = slave_config or {}
        
        # Redis connection
        self.redis = redis.from_url(redis_url)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        
        # Load the slave class dynamically
        module_path, class_name = slave_class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        self.slave_class = getattr(module, class_name)
        
        # Initialize pool
        self.slaves: List[AbstractSlave] = []
        self.last_used: List[float] = []  # Timestamp of last usage for each slave
        
        # Initialize slaves
        self._initialize_pool(initial_size)
        
        # Worker thread control
        self.running = False
        self.task_thread = None
        self.scaling_thread = None
        
        # Load balancer and health checker
        self.load_balancer = LoadBalancer()
        from utils.health_checker import \
            HealthChecker  # Move this import to break the circular dependency
        self.health_checker = HealthChecker(check_interval=30)
        
        # Task queue
        self.pending_tasks = []
        self.task_lock = threading.Lock()
        
        # Prometheus metrics
        self.pool_size = Gauge(
            f'{domain}_{slave_type}_pool_size',
            f'Number of slaves in the {domain}/{slave_type} pool'
        )
        self.pool_size.set(len(self.slaves))
        
        self.busy_slaves = Gauge(
            f'{domain}_{slave_type}_busy_slaves',
            f'Number of busy slaves in the {domain}/{slave_type} pool'
        )
        
        self.task_counter = Counter(
            f'{domain}_{slave_type}_pool_tasks',
            f'Tasks processed by the {domain}/{slave_type} pool',
            ['status']
        )
        
        logger.info(f"SlavePool for {domain}/{slave_type} initialized with {len(self.slaves)} slaves")
    
    def _initialize_pool(self, size: int):
        """
        Initialize the slave pool with the specified number of slaves.
        
        Args:
            size: Number of slaves to create
        """
        # Extract registry from config if available
        registry = self.slave_config.get("registry", None)
        
        for _ in range(size):
            try:
                slave = self.slave_class(config=self.slave_config, registry=registry)
                self.slaves.append(slave)
                self.last_used.append(0)
            except Exception as e:
                logger.error(f"Error creating slave {self.slave_type}: {e}")
    
    def start(self):
        """Start the slave pool worker threads."""
        if self.running:
            return
            
        # Subscribe to task channel
        self.pubsub.subscribe(f"slave_pool:{self.domain}:{self.slave_type}:tasks")
        
        # Start worker threads
        self.running = True
        
        # Thread for processing tasks
        self.task_thread = threading.Thread(target=self._process_tasks)
        self.task_thread.daemon = True
        self.task_thread.start()
        
        # Thread for auto-scaling
        self.scaling_thread = threading.Thread(target=self._auto_scale)
        self.scaling_thread.daemon = True
        self.scaling_thread.start()
        
        logger.info(f"SlavePool for {self.domain}/{self.slave_type} started")
    
    def stop(self):
        """Stop the slave pool worker threads."""
        self.running = False
        if self.task_thread:
            self.task_thread.join(timeout=1.0)
        if self.scaling_thread:
            self.scaling_thread.join(timeout=1.0)
        self.pubsub.unsubscribe()
        logger.info(f"SlavePool for {self.domain}/{self.slave_type} stopped")
    
    def _process_tasks(self):
        """
        Process tasks from Redis and the internal queue.
        Listens to the Redis pubsub channel and processes tasks.
        """
        while self.running:
            # First check for messages from Redis
            message = self.pubsub.get_message()
            if message and message["type"] == "message":
                try:
                    task = json.loads(message["data"])
                    with self.task_lock:
                        self.pending_tasks.append(task)
                        
                except Exception as e:
                    logger.error(f"Error parsing task in {self.domain}/{self.slave_type} pool: {e}")
            
            # Process any pending tasks
            with self.task_lock:
                if self.pending_tasks and self.slaves:
                    task = self.pending_tasks.pop(0)
                    # Process in a separate thread to avoid blocking
                    threading.Thread(
                        target=self._execute_task,
                        args=(task,)
                    ).start()
            
            # Small sleep to prevent CPU spinning
            time.sleep(0.01)
    
    def _execute_task(self, task: Dict[str, Any]):
        """
        Execute a task on an available slave.
        
        Args:
            task: Task to execute
        """
        task_id = task.get("task_id", "unknown")
        request_id = task.get("request_id", "unknown")
        
        # Get an available slave
        slave_index = self._get_available_slave()
        if slave_index is None:
            logger.warning(f"No available slaves in {self.domain}/{self.slave_type} pool for task {task_id}")
            with self.task_lock:
                self.pending_tasks.append(task)
            return
        
        # Mark slave as busy
        self.busy_slaves.inc()
        slave = self.slaves[slave_index]
        
        try:
            logger.debug(f"Executing task {task_id} on {self.domain}/{self.slave_type} slave")
            
            # Update last used timestamp
            self.last_used[slave_index] = time.time()
            
            # Execute the task
            start_time = time.time()
            result = slave.execute_task(task.get("parameters", {}))
            execution_time = time.time() - start_time
            
            # Add task metadata to the result
            result = {
                **result,
                "request_id": request_id,
                "task_id": task_id,
                "domain": self.domain,
                "slave_type": self.slave_type,
                "execution_time": execution_time
            }
            
            # Publish the result back to the domain master
            self.redis.publish(f"domain:{self.domain}:results", json.dumps(result))
            
            # Update metrics
            status = "success" if result.get("success", False) else "error"
            self.task_counter.labels(status=status).inc()
            
            logger.info(f"Task {task_id} completed by {self.domain}/{self.slave_type} slave in {execution_time:.2f}s")
            
        except Exception as e:
            # Handle execution error
            logger.error(f"Error executing task {task_id} on {self.domain}/{self.slave_type} slave: {e}")
            
            # Publish error result
            error_result = {
                "success": False,
                "error": str(e),
                "request_id": request_id,
                "task_id": task_id,
                "domain": self.domain,
                "slave_type": self.slave_type
            }
            
            self.redis.publish(f"domain:{self.domain}:results", json.dumps(error_result))
            self.task_counter.labels(status="error").inc()
            
        finally:
            # Mark slave as available
            self.busy_slaves.dec()
    
    def _get_available_slave(self) -> Optional[int]:
        """
        Get the index of an available slave using the load balancer.
        
        Returns:
            Index of an available slave, or None if none available
        """
        if not self.slaves:
            return None
            
        # Use load balancer to select a slave
        indices = list(range(len(self.slaves)))
        
        # Filter out any unhealthy slaves
        healthy_indices = [i for i in indices if self._is_slave_healthy(i)]
        
        if not healthy_indices:
            return None
            
        # Use load balancer to select from healthy slaves
        return self.load_balancer.select(healthy_indices)
    
    def _is_slave_healthy(self, index: int) -> bool:
        """
        Check if a slave is healthy.
        
        Args:
            index: Slave index
            
        Returns:
            Boolean indicating if the slave is healthy
        """
        if index >= len(self.slaves):
            return False
            
        slave = self.slaves[index]
        return slave.get_health()
    
    def _auto_scale(self):
        """
        Automatically adjust the slave pool size based on load.
        Scales up when high load, scales down when low load.
        """
        scaling_interval = 60  # Check every minute
        
        while self.running:
            try:
                # Calculate current load as the ratio of pending tasks to available slaves
                current_load = len(self.pending_tasks) / max(1, len(self.slaves) - self.busy_slaves._value.get())
                
                # Scale up if load is high and below max size
                if current_load > 0.8 and len(self.slaves) < self.max_size:
                    self._scale_up()
                
                # Scale down if load is low and we have more than minimum slaves
                elif current_load < 0.2 and len(self.slaves) > 2:
                    self._scale_down()
                
                # Replace any unhealthy slaves
                self._replace_unhealthy_slaves()
                
            except Exception as e:
                logger.error(f"Error in auto-scaling for {self.domain}/{self.slave_type} pool: {e}")
            
            time.sleep(scaling_interval)
    
    def _scale_up(self):
        """Add a new slave to the pool."""
        try:
            # Extract registry from config if available
            registry = self.slave_config.get("registry", None)
            
            slave = self.slave_class(config=self.slave_config, registry=registry)
            
            with self.task_lock:
                self.slaves.append(slave)
                self.last_used.append(time.time())
            
            self.pool_size.inc()
            logger.info(f"Scaled up {self.domain}/{self.slave_type} pool to {len(self.slaves)} slaves")
            
        except Exception as e:
            logger.error(f"Error scaling up {self.domain}/{self.slave_type} pool: {e}")
    
    def _scale_down(self):
        """Remove the least recently used slave from the pool."""
        with self.task_lock:
            if len(self.slaves) <= 1:
                return
                
            # Find the index of the least recently used slave
            lru_index = self.last_used.index(min(self.last_used))
            
            # Remove the slave
            del self.slaves[lru_index]
            del self.last_used[lru_index]
            
        self.pool_size.dec()
        logger.info(f"Scaled down {self.domain}/{self.slave_type} pool to {len(self.slaves)} slaves")
    
    def _replace_unhealthy_slaves(self):
        """Replace any unhealthy slaves in the pool."""
        # Extract registry from config if available
        registry = self.slave_config.get("registry", None)
        
        with self.task_lock:
            for i in range(len(self.slaves)):
                if not self._is_slave_healthy(i):
                    try:
                        logger.warning(f"Replacing unhealthy slave in {self.domain}/{self.slave_type} pool")
                        
                        # Create a new slave
                        self.slaves[i] = self.slave_class(config=self.slave_config, registry=registry)
                        self.last_used[i] = time.time()
                        
                    except Exception as e:
                        logger.error(f"Error replacing unhealthy slave in {self.domain}/{self.slave_type} pool: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the slave pool.
        
        Returns:
            Dictionary with status information
        """
        # Collect status from all slaves
        slave_statuses = []
        for i, slave in enumerate(self.slaves):
            try:
                slave_status = slave.report_status()
                slave_status["index"] = i
                slave_status["last_used"] = self.last_used[i]
                slave_status["healthy"] = slave.get_health()
                slave_statuses.append(slave_status)
            except Exception as e:
                logger.error(f"Error getting status from slave {i} in {self.domain}/{self.slave_type} pool: {e}")
        
        return {
            "domain": self.domain,
            "slave_type": self.slave_type,
            "pool_size": len(self.slaves),
            "busy_slaves": self.busy_slaves._value.get(),
            "pending_tasks": len(self.pending_tasks),
            "running": self.running,
            "slaves": slave_statuses
        }
    
    def get_health(self) -> bool:
        """
        Check if the slave pool is healthy.
        
        Returns:
            Boolean indicating if the pool is healthy
        """
        # Pool is healthy if at least one slave is healthy
        return any(self._is_slave_healthy(i) for i in range(len(self.slaves)))