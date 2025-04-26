import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict

import psutil
import redis
from prometheus_client import REGISTRY, Counter, Gauge, Histogram

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Check if metrics already exist in the registry to avoid duplicate registration
def safe_counter(name, documentation, labelnames=None):
    try:
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Counter(name, documentation, labelnames or [])
    except ValueError:
        # If metric already exists, return the existing one
        return REGISTRY._names_to_collectors[name]

def safe_gauge(name, documentation, labelnames=None):
    try:
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Gauge(name, documentation, labelnames or [])
    except ValueError:
        # If metric already exists, return the existing one
        return REGISTRY._names_to_collectors[name]

def safe_histogram(name, documentation, labelnames=None):
    try:
        if name in REGISTRY._names_to_collectors:
            return REGISTRY._names_to_collectors[name]
        return Histogram(name, documentation, labelnames or [])
    except ValueError:
        # If metric already exists, return the existing one
        return REGISTRY._names_to_collectors[name]

# System metrics
CPU_USAGE = safe_gauge('system_cpu_usage', 'System CPU usage percentage')
MEMORY_USAGE = safe_gauge('system_memory_usage_bytes', 'System memory usage in bytes')
DISK_USAGE = safe_gauge('system_disk_usage_bytes', 'System disk usage in bytes')
NETWORK_IO = safe_counter('system_network_io_bytes', 'System network IO in bytes', ['direction'])

# Master-Slave metrics
WORKFLOW_COUNTER = safe_counter(
    'nl2sparql_workflows_total',
    'Total number of NL2SPARQL workflows',
    ['status']
)

WORKFLOW_PROCESSING_TIME = safe_histogram(
    'nl2sparql_workflow_processing_seconds',
    'Time spent processing NL2SPARQL workflows',
    ['domain']
)

SLAVE_POOL_SIZE = safe_gauge(
    'slave_pool_size',
    'Number of slaves in a pool',
    ['domain', 'slave_type']
)

SLAVE_TASKS = safe_counter(
    'slave_tasks_total',
    'Total number of tasks processed by slaves',
    ['domain', 'slave_type', 'status']
)

# Domain Master specific metrics
DOMAIN_REQUEST_COUNT = safe_counter(
    'domain_requests_total',
    'Total number of requests processed by domain',
    ['domain', 'status']
)

DOMAIN_PROCESSING_TIME = safe_histogram(
    'domain_processing_seconds',
    'Time spent processing in domain',
    ['domain', 'task_type']
)

ACTIVE_WORKFLOWS = safe_gauge(
    'active_workflows',
    'Number of active workflows',
    ['domain']
)

SLAVE_TASK_COUNT = safe_counter(
    'slave_tasks_total',
    'Total number of tasks processed by slaves',
    ['slave_type', 'status']
)

# Agent metrics
AGENT_EXECUTION_TIME = safe_histogram(
    'agent_execution_seconds',
    'Time spent in agent execution',
    ['agent_type', 'method']
)

AGENT_ERRORS = safe_counter(
    'agent_errors_total',
    'Total number of agent execution errors',
    ['agent_type', 'error_type']
)

AGENT_MEMORY_USAGE = safe_gauge(
    'agent_memory_usage_bytes',
    'Agent memory usage in bytes',
    ['agent_type']
)

class SystemMonitor:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SystemMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, update_interval: float = 5.0):
        if self._initialized:
            return
        self.update_interval = update_interval
        self.running = False
        self.monitor_thread = None
        # Prometheus metrics - use our safe registration functions
        self.cpu_gauge = safe_gauge('system_cpu_usage', 'System CPU usage percentage')
        self.memory_gauge = safe_gauge('system_memory_usage_bytes', 'System memory usage in bytes')
        self.disk_gauge = safe_gauge('system_disk_usage_bytes', 'System disk usage in bytes')
        # Process info
        self.process = psutil.Process(os.getpid())
        self.hostname = os.uname().nodename if hasattr(os, 'uname') else 'unknown'
        self._initialized = True

    def start(self):
        """Start the system monitor"""
        if self.running:
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        logger.info("System monitor started")
    
    def stop(self):
        """Stop the system monitor"""
        self.running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
            
        logger.info("System monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                # Update CPU usage
                cpu_percent = psutil.cpu_percent(interval=1.0)
                self.cpu_gauge.set(cpu_percent)
                
                # Update memory usage
                memory_info = self.process.memory_info()
                self.memory_gauge.set(memory_info.rss)
                
                # Update disk usage
                disk = psutil.disk_usage('/')
                self.disk_gauge.set(disk.used)
                
                # Log summary
                logger.debug(f"System: CPU={cpu_percent}%, MEM={memory_info.rss/1024/1024:.1f}MB, DISK={disk.used/1024/1024/1024:.1f}GB")
                
            except Exception as e:
                logger.error(f"Error in system monitor: {e}")
                
            # Sleep for the update interval
            time.sleep(self.update_interval)

class MetricsLogger:
    """
    Class for logging and tracking workflow metrics.
    Provides methods for storing and retrieving metrics for workflows and domains.
    """
    
    def __init__(self, redis_url: str = None, expiry: int = 3600):
        """
        Initialize the metrics logger.
        
        Args:
            redis_url: Redis URL for metrics storage
            expiry: Time in seconds to keep metrics in Redis (default: 1 hour)
        """
        self.redis_url = redis_url
        self.expiry = expiry
        self.redis = None
        self.metrics = {}
        
        # Only connect to Redis if URL is provided
        if redis_url:
            try:
                self.redis = redis.from_url(redis_url)
                logger.info("Connected to Redis for metrics logging")
            except Exception as e:
                logger.error(f"Failed to connect to Redis for metrics: {e}")
    
    def log_metric(self, workflow_id: str, metric_name: str, value: Any):
        """Log a metric for a workflow"""
        if self.redis:
            # Store in Redis
            metrics_key = f"metrics:{workflow_id}"
            
            # Get existing metrics if any
            metrics_json = self.redis.get(metrics_key)
            if metrics_json:
                metrics = json.loads(metrics_json)
            else:
                metrics = {}
            
            # Update metrics
            metrics[metric_name] = value
            
            # Store back
            self.redis.set(metrics_key, json.dumps(metrics), ex=self.expiry)
        else:
            # Store in memory
            if workflow_id not in self.metrics:
                self.metrics[workflow_id] = {}
            
            self.metrics[workflow_id][metric_name] = value
    
    def get_workflow_metrics(self, workflow_id: str) -> Dict[str, Any]:
        """Get metrics for a workflow"""
        if self.redis:
            # Get from Redis
            metrics_key = f"metrics:{workflow_id}"
            metrics_json = self.redis.get(metrics_key)
            
            if metrics_json:
                return json.loads(metrics_json)
            return {}
        else:
            # Get from memory
            return self.metrics.get(workflow_id, {})
    
    def log_timing(self, workflow_id: str, stage: str, duration: float):
        """Log timing information for a workflow stage"""
        self.log_metric(workflow_id, f"timing:{stage}", duration)
    
    def log_count(self, workflow_id: str, counter: str, count: int = 1):
        """Log count information for a workflow"""
        current = self.get_workflow_metrics(workflow_id).get(f"count:{counter}", 0)
        self.log_metric(workflow_id, f"count:{counter}", current + count)
    
    def log_memory_usage(self, workflow_id: str, component: str, memory_mb: float):
        """Log memory usage for a component"""
        self.log_metric(workflow_id, f"memory:{component}", memory_mb)
    
    def log_error(self, workflow_id: str, error_type: str, details: str):
        """Log error information for a workflow"""
        self.log_metric(workflow_id, f"error:{error_type}", details)
        self.log_count(workflow_id, "errors")


class HealthCheck:
    """
    Class for performing health checks on services.
    """
    
    def __init__(self):
        """Initialize the health check service"""
        self.checks = {}
        self.lock = threading.Lock()
    
    def register_service(self, service_name: str, check_func: Callable):
        """
        Register a new service health check.
        
        Args:
            service_name: Name of the service
            check_func: Function to call to check health (should return or raise an exception on failure)
        """
        with self.lock:
            self.checks[service_name] = check_func
            logger.info(f"Registered health check for {service_name}")
    
    async def check_health(self) -> Dict[str, str]:
        """
        Check health of all registered services.
        
        Returns:
            Dict of service names to status ("healthy" or "unhealthy")
        """
        results = {}
        
        # Make a copy of the checks dict to avoid issues with concurrent modifications
        checks_copy = None
        with self.lock:
            checks_copy = dict(self.checks)
        
        for service_name, check_func in checks_copy.items():
            try:
                await check_func()
                results[service_name] = "healthy"
            except Exception as e:
                logger.warning(f"Health check for {service_name} failed: {e}")
                results[service_name] = "unhealthy"
        
        return results

# Health check functions for specific services

async def check_slave_pool_health(slave_pool_manager) -> bool:
    """
    Check the health of slave pools.
    
    Args:
        slave_pool_manager: The slave pool manager instance
        
    Returns:
        True if healthy, raises exception otherwise
    """
    # Check if slave pool manager is initialized
    if not slave_pool_manager:
        raise Exception("Slave pool manager not initialized")
        
    # Check all slave pools
    unhealthy_pools = []
    for pool_name, pool in slave_pool_manager.pools.items():
        if not await pool.is_healthy():
            unhealthy_pools.append(pool_name)
            
    if unhealthy_pools:
        raise Exception(f"Unhealthy slave pools: {', '.join(unhealthy_pools)}")
        
    return True

async def check_redis_health(redis_url: str) -> bool:
    """
    Check Redis health.
    
    Args:
        redis_url: The Redis connection URL
        
    Returns:
        True if healthy, raises exception otherwise
    """
    try:
        r = redis.from_url(redis_url)
        r.ping()
        return True
    except Exception as e:
        raise Exception(f"Redis health check failed: {str(e)}")

async def check_database_health(db_client) -> bool:
    """
    Check database health.
    
    Args:
        db_client: Database client instance
        
    Returns:
        True if healthy, raises exception otherwise
    """
    try:
        # Check connection status
        connection_status = await db_client.check_connection()
        if not connection_status:
            raise Exception("Database connection failed")
        return True
    except Exception as e:
        raise Exception(f"Database health check failed: {str(e)}")

def register_health_checks(health_checker, slave_pool_manager, redis_url, db_client=None):
    """
    Register all health checks with the health checker.
    
    Args:
        health_checker: The health checker instance
        slave_pool_manager: The slave pool manager instance
        redis_url: The Redis connection URL
        db_client: The database client instance
    """
    # Register slave pool health check
    health_checker.register_service("slave_pools", 
        lambda: check_slave_pool_health(slave_pool_manager))
    
    # Register Redis health check
    health_checker.register_service("redis", 
        lambda: check_redis_health(redis_url))
    
    # Register database health check if client provided
    if db_client:
        health_checker.register_service("database", 
            lambda: check_database_health(db_client))
    
    logger.info("All health checks registered")

# Initialize global instances
metrics_logger = MetricsLogger()
health_check = HealthCheck()
system_monitor = SystemMonitor()

# Convenience functions for logging workflow metrics

def log_workflow_start(workflow_id: str, query: str):
    """Log the start of a workflow"""
    metrics_logger.log_metric(workflow_id, "start_time", time.time())
    metrics_logger.log_metric(workflow_id, "query", query)
    metrics_logger.log_count(workflow_id, "workflows")
    logger.info(f"Workflow {workflow_id} started for query: {query}")

def log_workflow_completion(workflow_id: str, success: bool, duration: float):
    """Log the completion of a workflow"""
    metrics_logger.log_metric(workflow_id, "end_time", time.time())
    metrics_logger.log_metric(workflow_id, "duration", duration)
    metrics_logger.log_metric(workflow_id, "success", success)
    
    if success:
        metrics_logger.log_count(workflow_id, "successful_workflows")
    else:
        metrics_logger.log_count(workflow_id, "failed_workflows")
        
    logger.info(f"Workflow {workflow_id} completed in {duration:.2f}s with success={success}")

def log_domain_processing(workflow_id: str, domain: str, duration: float):
    """Log domain processing time"""
    metrics_logger.log_timing(workflow_id, f"domain:{domain}", duration)
    logger.info(f"Workflow {workflow_id}: {domain} domain completed in {duration:.2f}s")

def log_slave_action(workflow_id: str, slave_type: str, action: str, duration: float):
    """Log slave action time"""
    metrics_logger.log_timing(workflow_id, f"slave:{slave_type}:{action}", duration)
    logger.debug(f"Workflow {workflow_id}: {slave_type} slave {action} in {duration:.2f}s")

def log_task_execution(workflow_id: str, task_name: str, success: bool, duration: float):
    """Log task execution time"""
    metrics_logger.log_timing(workflow_id, f"task:{task_name}", duration)
    metrics_logger.log_metric(workflow_id, f"task:{task_name}:success", success)
    
    if success:
        metrics_logger.log_count(workflow_id, "successful_tasks")
    else:
        metrics_logger.log_count(workflow_id, "failed_tasks")
        
    logger.debug(f"Workflow {workflow_id}: task {task_name} completed in {duration:.2f}s with success={success}")

def log_slave_pool_size(domain: str, slave_type: str, size: int):
    """Log the size of a slave pool to Prometheus metrics"""
    SLAVE_POOL_SIZE.labels(domain=domain, slave_type=slave_type).set(size)
    logger.debug(f"Slave pool {domain}/{slave_type} size updated to {size}")

# System monitoring functions

def start_monitoring(redis_url: str = None):
    """
    Start the monitoring systems.
    
    Args:
        redis_url: Optional Redis URL for storing metrics
    """
    # Initialize metrics logger with Redis if URL provided
    if redis_url:
        global metrics_logger
        metrics_logger = MetricsLogger(redis_url)
        
        # Initialize Redis client and start workflow metrics update thread
        try:
            redis_client = redis.from_url(redis_url)
            thread = threading.Thread(target=update_workflow_metrics, args=(redis_client,))
            thread.daemon = True
            thread.start()
            logger.info("Workflow metrics update thread started")
        except Exception as e:
            logger.error(f"Failed to start workflow metrics update thread: {e}")
    
    # Start system monitor
    system_monitor.start()
    
    logger.info("Monitoring systems started")

def stop_monitoring():
    """Stop the monitoring systems"""
    system_monitor.stop()
    logger.info("Monitoring systems stopped")

# Function to periodically update workflow metrics from Redis
def update_workflow_metrics(redis_client):
    """
    Update workflow metrics from Redis.
    
    This function periodically counts active workflows by domain and updates
    the ACTIVE_WORKFLOWS gauge to reflect current workflow processing state.
    
    Args:
        redis_client: Redis client instance for querying workflow keys
    """
    while True:
        try:
            # Count active workflows by domain
            nlp_workflows = len(redis_client.keys("workflow:*:nlp:active"))
            query_workflows = len(redis_client.keys("workflow:*:query:active"))
            response_workflows = len(redis_client.keys("workflow:*:response:active"))
            
            # Update Prometheus gauges
            ACTIVE_WORKFLOWS.labels(domain="nlp").set(nlp_workflows)
            ACTIVE_WORKFLOWS.labels(domain="query").set(query_workflows)
            ACTIVE_WORKFLOWS.labels(domain="response").set(response_workflows)
            
            # Log metrics update
            logger.debug(f"Updated workflow metrics - NLP: {nlp_workflows}, Query: {query_workflows}, Response: {response_workflows}")
            
        except Exception as e:
            logger.error(f"Error updating workflow metrics: {e}")
        
        time.sleep(15)  # Update every 15 seconds