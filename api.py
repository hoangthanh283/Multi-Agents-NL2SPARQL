import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import redis
import uvicorn
from celery.result import AsyncResult
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (CONTENT_TYPE_LATEST, Counter, Histogram,
                               generate_latest)
from pydantic import BaseModel
from rdflib import Graph
from starlette.responses import Response

from database.ontology_store import OntologyStore
from tasks import (execute_sparql, get_ontology_summary, search_classes,
                   search_instances, search_properties)
from utils.monitoring import (health_check, metrics_logger, start_monitoring,
                              stop_monitoring)
from utils.rate_limiter import circuit_break, rate_limit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total number of HTTP requests',
    ['method', 'endpoint', 'status']
)

REQUEST_LATENCY = Histogram(
    'http_request_duration_seconds',
    'HTTP request latency in seconds',
    ['method', 'endpoint']
)

QUERY_COUNT = Counter(
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
redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

# Initialize thread pool
executor = ThreadPoolExecutor(max_workers=10)

# Initialize OntologyStore
ontology_store = OntologyStore(
    endpoint_url="http://localhost:7200/repositories/CHeVIE",
    redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    max_workers=10
)

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

class TaskResponse(BaseModel):
    task_id: str

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Start system monitoring
        start_monitoring()
        
        # Register health checks
        health_check.register_service("redis", check_redis_health)
        health_check.register_service("graphdb", check_graphdb_health)
        health_check.register_service("celery", check_celery_health)
        
        # Load ontology
        success = ontology_store.load_ontology()
        if not success:
            logger.error("Failed to load ontology")
    except Exception as e:
        logger.error(f"Error during startup: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    stop_monitoring()

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

@app.get("/health")
async def health():
    """Health check endpoint"""
    results = await health_check.check_health()
    is_healthy = all(status == "healthy" for status in results.values())
    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "services": results
    }

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

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)