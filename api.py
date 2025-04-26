import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import redis
import uvicorn
from celery.result import AsyncResult
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (CONTENT_TYPE_LATEST, REGISTRY, Counter, Gauge,
                               Histogram, generate_latest)
from pydantic import BaseModel
from starlette.responses import Response

from database.ontology_store import OntologyStore
from master.global_master import GlobalMaster
from slaves.slave_pool_manager import SlavePoolManager
from tasks import (execute_sparql, get_ontology_summary, search_classes,
                   search_instances, search_properties)
from utils.monitoring import (ACTIVE_WORKFLOWS, DOMAIN_PROCESSING_TIME,
                              SLAVE_POOL_SIZE, SLAVE_TASK_COUNT,
                              WORKFLOW_COUNTER, health_check, metrics_logger,
                              register_health_checks, start_monitoring,
                              stop_monitoring)
from utils.rate_limiter import circuit_break, rate_limit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_or_create_metric(metric_cls, name, *args, **kwargs):
    try:
        # Try to get the metric if it already exists
        return REGISTRY._names_to_collectors[name]
    except KeyError:
        # Otherwise, create it
        return metric_cls(name, *args, **kwargs)

CPU_USAGE = get_or_create_metric(Gauge, 'system_cpu_usage', 'System CPU usage percentage')
MEMORY_USAGE = get_or_create_metric(Gauge, 'system_memory_usage_bytes', 'System memory usage in bytes')
DISK_USAGE = get_or_create_metric(Gauge, 'system_disk_usage_bytes', 'System disk usage in bytes')
NETWORK_IO = get_or_create_metric(Counter, 'system_network_io_bytes', 'System network IO in bytes', ['direction'])

# Define Prometheus metrics
REQUEST_COUNT = get_or_create_metric(Counter,
    'http_requests_total',
    'Total number of HTTP requests',
    ['method', 'endpoint', 'status']
)

REQUEST_LATENCY = get_or_create_metric(Histogram,
    'http_request_duration_seconds',
    'HTTP request latency in seconds',
    ['method', 'endpoint']
)

QUERY_COUNT = get_or_create_metric(Counter,
    'sparql_queries_total',
    'Total number of SPARQL queries executed',
    ['type', 'status']
)

# Initialize FastAPI app
app = FastAPI(title="Multi-Agents NL2SPARQL API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis client
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(redis_url)

# Initialize thread pool
executor = ThreadPoolExecutor(max_workers=10)

# Initialize OntologyStore
ontology_store = OntologyStore(
    endpoint_url=os.getenv("GRAPHDB_URL", "http://localhost:7200/repositories/CHeVIE"),
    redis_url=redis_url,
    max_workers=10
)

# Initialize Global Master and Slave Pool Manager
global_master = GlobalMaster(redis_url=redis_url, endpoint_url=os.getenv("GRAPHDB_URL", "http://localhost:7200/repositories/CHeVIE"))
slave_pool_manager = None  # Will be initialized in startup event

# Configure slave pool settings
slave_pool_configs = {
    "nlp.query_refinement": {"initial_size": 2, "max_size": 5},
    "nlp.entity_recognition": {"initial_size": 2, "max_size": 5},
    "query.ontology_mapping": {"initial_size": 2, "max_size": 5},
    "query.sparql_construction": {"initial_size": 2, "max_size": 5},
    "query.validation": {"initial_size": 1, "max_size": 3},
    "response.query_execution": {"initial_size": 2, "max_size": 5, "slave_config": {"endpoint_url": os.getenv("GRAPHDB_URL", "http://localhost:7200/repositories/CHeVIE")}},
    "response.response_generation": {"initial_size": 2, "max_size": 5},
}

@app.middleware("http")
async def add_metrics(request: Request, call_next):
    """Middleware to collect request metrics"""
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    REQUEST_LATENCY.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    return response

@app.get("/metrics")
def metrics():
    """Endpoint for Prometheus metrics"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

class SPARQLQuery(BaseModel):
    query: str

class SearchQuery(BaseModel):
    query: str
    limit: Optional[int] = 10
    threshold: Optional[float] = 0.5

class NLQuery(BaseModel):
    query: str
    context: Optional[List[str]] = None

class TaskResponse(BaseModel):
    task_id: str

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global slave_pool_manager, global_master
    try:
        # Initialize the slave pool manager with the configured pools
        slave_pool_manager = SlavePoolManager(
            redis_url=redis_url,
            pool_configs=slave_pool_configs
        )
        
        # Start the global master and domain masters
        global_master.start()
        
        # Start the slave pools
        await slave_pool_manager.start_all_pools()
        
        # Start system monitoring with Redis URL for metrics collection
        start_monitoring(redis_url=redis_url)
        
        # Register health checks
        register_health_checks(
            health_checker=health_check,
            slave_pool_manager=slave_pool_manager,
            redis_url=redis_url,
            db_client=ontology_store
        )
        
        logger.info("API startup completed successfully")
    except Exception as e:
        logger.error(f"Error during API startup: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    try:
        # Stop global master and slave pools
        global_master.stop()
        if slave_pool_manager:
            slave_pool_manager.stop_all_pools()
            
        stop_monitoring()
        logger.info("Master-Slave architecture stopped")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Health check implementations
async def check_redis_health():
    try:
        redis_client.ping()
    except Exception as e:
        raise Exception(f"Redis health check failed: {e}")

async def check_graphdb_health():
    try:
        test_query = "ASK { ?s ?p ?o }"
        result = ontology_store.execute_sparql(test_query)
        if not result.get("success", False):
            raise Exception("GraphDB query failed")
    except Exception as e:
        raise Exception(f"GraphDB health check failed: {e}")

async def check_celery_health():
    try:
        i = celery_app.control.inspect()
        if not i.active():
            raise Exception("No active Celery workers found")
    except Exception as e:
        raise Exception(f"Celery health check failed: {e}")

async def check_master_slave_health():
    try:
        # Check global master health
        master_health = global_master.get_health()
        
        # Check slave pools health
        if slave_pool_manager:
            pools_health = slave_pool_manager.get_health()
            
            # Check if at least one pool is healthy in each domain
            domains_health = all(health for domain, health in pools_health.items())
            
            if not master_health.get("status") == "healthy" or not domains_health:
                raise Exception("Some master or slave components are unhealthy")
    except Exception as e:
        raise Exception(f"Master-Slave health check failed: {e}")

@app.get("/health")
async def health():
    """Health check endpoint"""
    results = await health_check.check_health()
    is_healthy = all(status == "healthy" for status in results.values())
    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "services": results
    }

# New endpoints for NL2SPARQL using the master-slave architecture

@app.post("/api/nl2sparql", response_model=TaskResponse)
@rate_limit(redis_client, lambda q: "nl2sparql", max_requests=50, time_window=60)
@circuit_break(redis_client, "nl2sparql", failure_threshold=5)
async def nl_to_sparql(query: NLQuery):
    """Process natural language query using the master-slave architecture"""
    try:
        # Create a workflow using the global master
        request_id = global_master.create_workflow(query.query, query.context)
        
        # Start the workflow asynchronously
        global_master.start_workflow(request_id)
        
        return {"task_id": request_id}
    except Exception as e:
        logger.error(f"Error in nl_to_sparql: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/nl2sparql/{workflow_id}/status")
async def get_workflow_status(workflow_id: str):
    """Get the current status of an NL2SPARQL workflow"""
    try:
        # Get status from global master
        status = global_master.get_workflow_status(workflow_id)
        if not status:
            raise HTTPException(status_code=404, detail="Workflow not found")
            
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_workflow_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/nl2sparql/{workflow_id}/result")
async def get_workflow_result(workflow_id: str):
    """Get the result of a completed NL2SPARQL workflow"""
    try:
        # Get result from global master
        result = global_master.get_workflow_result(workflow_id)
        if not result:
            raise HTTPException(status_code=404, detail="Workflow result not found")
            
        if not result.get("completed", False):
            return {
                "status": "pending",
                "message": "Workflow is still processing"
            }
            
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_workflow_result: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/master/health")
async def master_health():
    """Get health status of the master-slave architecture components"""
    try:
        # Get master health
        master_health = global_master.get_health()
        
        # Get slave pools health if available
        pools_health = {}
        if slave_pool_manager:
            pools_health = slave_pool_manager.get_health()
            
        return {
            "global_master": master_health,
            "slave_pools": pools_health
        }
    except Exception as e:
        logger.error(f"Error in master_health: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/master/status")
async def master_status():
    """Get detailed status of master-slave architecture components"""
    try:
        # Get domain masters status
        domain_masters_status = {}
        for domain, master in global_master.domain_masters.items():
            domain_masters_status[domain] = master.get_status()
        
        # Get active workflows
        active_workflows = len(global_master.active_workflows)
        
        # Get slave pools status if available
        pools_status = {}
        if slave_pool_manager:
            pools_status = slave_pool_manager.get_status()
            
        return {
            "global_master": {
                "active": global_master.running,
                "active_workflows": active_workflows
            },
            "domain_masters": domain_masters_status,
            "slave_pools": pools_status
        }
    except Exception as e:
        logger.error(f"Error in master_status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sparql", response_model=TaskResponse)
@rate_limit(redis_client, lambda q: "sparql", max_requests=100, time_window=60)
@circuit_break(redis_client, "sparql", failure_threshold=5)
async def execute_sparql_query(query: SPARQLQuery):
    """Execute a SPARQL query asynchronously with rate limiting and circuit breaker"""
    try:
        task = execute_sparql.delay(query.query)
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search/classes", response_model=TaskResponse)
@rate_limit(redis_client, lambda q: "search_classes", max_requests=200, time_window=60)
@circuit_break(redis_client, "search_classes", failure_threshold=5)
async def search_ontology_classes(query: SearchQuery):
    """Search for ontology classes asynchronously with rate limiting"""
    try:
        task = search_classes.delay(query.query, query.limit, query.threshold)
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search/properties", response_model=TaskResponse)
@rate_limit(redis_client, lambda q: "search_properties", max_requests=200, time_window=60)
@circuit_break(redis_client, "search_properties", failure_threshold=5)
async def search_ontology_properties(query: SearchQuery):
    """Search for ontology properties asynchronously"""
    try:
        task = search_properties.delay(query.query, query.limit, query.threshold)
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/search/instances", response_model=TaskResponse)
@rate_limit(redis_client, lambda q: "search_instances", max_requests=200, time_window=60)
@circuit_break(redis_client, "search_instances", failure_threshold=5)
async def search_ontology_instances(query: SearchQuery):
    """Search for ontology instances asynchronously"""
    try:
        task = search_instances.delay(query.query, query.limit, query.threshold)
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ontology/summary", response_model=TaskResponse)
async def get_ontology_summary_async():
    """Get ontology summary asynchronously"""
    try:
        task = get_ontology_summary.delay()
        return {"task_id": task.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tasks/{task_id}")
async def get_task_result(task_id: str):
    """Get the result of an asynchronous task"""
    try:
        task = AsyncResult(task_id)
        if task.ready():
            result = task.get()
            if isinstance(result, dict) and not result.get("success", True):
                raise HTTPException(status_code=400, detail=result.get("error", "Unknown error"))
            return result
        return {"status": "pending"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/system")
async def get_system_metrics():
    """Get current system metrics"""
    try:
        metrics = {
            "cpu_usage": float(CPU_USAGE._value.get()),
            "memory_usage": float(MEMORY_USAGE._value.get()),
            "disk_usage": float(DISK_USAGE._value.get()),
        }
        metrics_logger.log_metrics(metrics)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/domains")
async def get_domain_metrics():
    """Get domain-specific metrics for the master-slave architecture"""
    try:
        # Collect active workflows by domain
        active_workflows = {
            "nlp": int(ACTIVE_WORKFLOWS.labels(domain="nlp")._value.get()),
            "query": int(ACTIVE_WORKFLOWS.labels(domain="query")._value.get()),
            "response": int(ACTIVE_WORKFLOWS.labels(domain="response")._value.get())
        }
        
        # Get domain task counts from Redis
        domain_tasks = {
            "nlp": {
                "total": redis_client.get("metrics:tasks:nlp:total") or 0,
                "success": redis_client.get("metrics:tasks:nlp:success") or 0,
                "error": redis_client.get("metrics:tasks:nlp:error") or 0
            },
            "query": {
                "total": redis_client.get("metrics:tasks:query:total") or 0,
                "success": redis_client.get("metrics:tasks:query:success") or 0,
                "error": redis_client.get("metrics:tasks:query:error") or 0
            },
            "response": {
                "total": redis_client.get("metrics:tasks:response:total") or 0,
                "success": redis_client.get("metrics:tasks:response:success") or 0,
                "error": redis_client.get("metrics:tasks:response:error") or 0
            }
        }
        
        # Get slave pool statistics
        slave_pool_stats = {}
        if slave_pool_manager:
            slave_pool_stats = slave_pool_manager.get_statistics()
        
        return {
            "active_workflows": active_workflows,
            "domain_tasks": domain_tasks,
            "slave_pools": slave_pool_stats
        }
    except Exception as e:
        logger.error(f"Error in get_domain_metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/dashboard")
async def get_metrics_dashboard():
    """Get comprehensive metrics for dashboard visualization"""
    try:
        # System metrics
        system_metrics = {
            "cpu_usage": float(CPU_USAGE._value.get()),
            "memory_usage_mb": float(MEMORY_USAGE._value.get()) / (1024 * 1024),
            "disk_usage_gb": float(DISK_USAGE._value.get()) / (1024 * 1024 * 1024)
        }
        
        # Workflow metrics
        workflows_total = {
            "total": int(WORKFLOW_COUNTER.labels(status="started")._value.get()),
            "completed": int(WORKFLOW_COUNTER.labels(status="completed")._value.get()),
            "error": int(WORKFLOW_COUNTER.labels(status="error")._value.get())
        }
        
        # Active workflows by domain
        active_workflows = {
            "total": sum(int(ACTIVE_WORKFLOWS.labels(domain=domain)._value.get()) 
                       for domain in ["nlp", "query", "response"]),
            "by_domain": {
                "nlp": int(ACTIVE_WORKFLOWS.labels(domain="nlp")._value.get()),
                "query": int(ACTIVE_WORKFLOWS.labels(domain="query")._value.get()),
                "response": int(ACTIVE_WORKFLOWS.labels(domain="response")._value.get())
            }
        }
        
        # Processing time histograms
        # Note: We're extracting the sum and count from histogram metrics
        processing_times = {
            "nlp": {
                "sum": float(DOMAIN_PROCESSING_TIME.labels(domain="nlp", task_type="total")._sum.get()),
                "count": float(DOMAIN_PROCESSING_TIME.labels(domain="nlp", task_type="total")._count.get())
            },
            "query": {
                "sum": float(DOMAIN_PROCESSING_TIME.labels(domain="query", task_type="total")._sum.get()),
                "count": float(DOMAIN_PROCESSING_TIME.labels(domain="query", task_type="total")._count.get())
            },
            "response": {
                "sum": float(DOMAIN_PROCESSING_TIME.labels(domain="response", task_type="total")._sum.get()),
                "count": float(DOMAIN_PROCESSING_TIME.labels(domain="response", task_type="total")._count.get())
            }
        }
        
        # Task counters
        task_counters = {
            "nlp": {
                "query_refinement": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="query_refinement", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="query_refinement", status="error")._value.get())
                },
                "entity_recognition": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="entity_recognition", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="entity_recognition", status="error")._value.get())
                }
            },
            "query": {
                "ontology_mapping": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="ontology_mapping", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="ontology_mapping", status="error")._value.get())
                },
                "sparql_construction": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="sparql_construction", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="sparql_construction", status="error")._value.get())
                },
                "validation": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="validation", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="validation", status="error")._value.get())
                }
            },
            "response": {
                "query_execution": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="query_execution", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="query_execution", status="error")._value.get())
                },
                "response_generation": {
                    "success": int(SLAVE_TASK_COUNT.labels(slave_type="response_generation", status="success")._value.get()),
                    "error": int(SLAVE_TASK_COUNT.labels(slave_type="response_generation", status="error")._value.get())
                }
            }
        }
        
        # Get slave pool stats
        slave_pools = {}
        if slave_pool_manager:
            # Get current pool sizes
            for pool_name, config in slave_pool_configs.items():
                domain, slave_type = pool_name.split('.', 1)
                if domain not in slave_pools:
                    slave_pools[domain] = {}
                
                pool_size = int(SLAVE_POOL_SIZE.labels(domain=domain, slave_type=slave_type)._value.get())
                slave_pools[domain][slave_type] = {
                    "size": pool_size,
                    "max_size": config["max_size"],
                    "utilization": pool_size / config["max_size"] if config["max_size"] > 0 else 0
                }
        
        # API request metrics
        api_requests = {
            "total": int(REQUEST_COUNT.labels(method="POST", endpoint="/api/nl2sparql", status=200)._value.get()),
            "error": int(REQUEST_COUNT.labels(method="POST", endpoint="/api/nl2sparql", status=500)._value.get()),
            "average_latency": float(REQUEST_LATENCY.labels(method="POST", endpoint="/api/nl2sparql")._sum.get()) / 
                              (float(REQUEST_LATENCY.labels(method="POST", endpoint="/api/nl2sparql")._count.get()) or 1)
        }
        
        return {
            "timestamp": time.time(),
            "system": system_metrics,
            "workflows": {
                "total": workflows_total,
                "active": active_workflows
            },
            "processing_times": processing_times,
            "tasks": task_counters,
            "slave_pools": slave_pools,
            "api": api_requests
        }
    except Exception as e:
        logger.error(f"Error in get_metrics_dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
