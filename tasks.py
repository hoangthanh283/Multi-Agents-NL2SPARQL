import logging
import os

from celery import Celery
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from adapters.agent_adapter import AgentAdapter
from agents.entity_recognition import EntityRecognitionAgent
from agents.ontology_mapping import OntologyMappingAgent
from agents.query_execution import QueryExecutionAgent
from agents.query_refinement import QueryRefinementAgent
from agents.response_generation import ResponseGenerationAgent
from agents.sparql_construction import SPARQLConstructionAgent
from agents.sparql_validation import SparqlValidationAgent
from database.ontology_store import OntologyStore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics for Celery
TASK_COUNT = Counter(
    'celery_tasks_total',
    'Total number of Celery tasks',
    ['task_name', 'status']
)

TASK_LATENCY = Histogram(
    'celery_task_duration_seconds',
    'Task execution time in seconds',
    ['task_name']
)

WORKER_TASKS_PENDING = Gauge(
    'celery_worker_tasks_pending',
    'Number of tasks currently in queue'
)

# Start Prometheus metrics server
start_http_server(8000)

# Initialize Celery
celery_app = Celery('nl2sparql_tasks',
                    broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/1'),
                    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/2'))

# Initialize OntologyStore
ontology_store = OntologyStore(
    endpoint_url=os.getenv('GRAPHDB_URL', "http://localhost:7200/repositories/CHeVIE"),
    redis_url=os.getenv('REDIS_URL', "redis://localhost:6379/0")
)

@celery_app.task(name='execute_sparql')
def execute_sparql(query: str):
    """Execute SPARQL query asynchronously"""
    try:
        TASK_COUNT.labels(task_name='execute_sparql', status='started').inc()
        with TASK_LATENCY.labels(task_name='execute_sparql').time():
            result = ontology_store.execute_sparql(query)
        TASK_COUNT.labels(task_name='execute_sparql', status='success').inc()
        return result
    except Exception as e:
        logger.error(f"Error executing SPARQL query: {e}")
        TASK_COUNT.labels(task_name='execute_sparql', status='error').inc()
        return {"success": False, "error": str(e)}

@celery_app.task(name='search_classes')
def search_classes(query: str, limit: int = 10, threshold: float = 0.5):
    """Search for ontology classes asynchronously"""
    try:
        TASK_COUNT.labels(task_name='search_classes', status='started').inc()
        with TASK_LATENCY.labels(task_name='search_classes').time():
            results = ontology_store.search_classes(query, limit=limit, threshold=threshold)
        TASK_COUNT.labels(task_name='search_classes', status='success').inc()
        return {"success": True, "results": results}
    except Exception as e:
        logger.error(f"Error searching classes: {e}")
        TASK_COUNT.labels(task_name='search_classes', status='error').inc()
        return {"success": False, "error": str(e)}

@celery_app.task(name='search_properties')
def search_properties(query: str, limit: int = 10, threshold: float = 0.5):
    """Search for ontology properties asynchronously"""
    try:
        TASK_COUNT.labels(task_name='search_properties', status='started').inc()
        with TASK_LATENCY.labels(task_name='search_properties').time():
            results = ontology_store.search_properties(query, limit=limit, threshold=threshold)
        TASK_COUNT.labels(task_name='search_properties', status='success').inc()
        return {"success": True, "results": results}
    except Exception as e:
        logger.error(f"Error searching properties: {e}")
        TASK_COUNT.labels(task_name='search_properties', status='error').inc()
        return {"success": False, "error": str(e)}

@celery_app.task(name='search_instances')
def search_instances(query: str, limit: int = 10, threshold: float = 0.5):
    """Search for ontology instances asynchronously"""
    try:
        TASK_COUNT.labels(task_name='search_instances', status='started').inc()
        with TASK_LATENCY.labels(task_name='search_instances').time():
            results = ontology_store.search_instances(query, limit=limit, threshold=threshold)
        TASK_COUNT.labels(task_name='search_instances', status='success').inc()
        return {"success": True, "results": results}
    except Exception as e:
        logger.error(f"Error searching instances: {e}")
        TASK_COUNT.labels(task_name='search_instances', status='error').inc()
        return {"success": False, "error": str(e)}

@celery_app.task(name='get_ontology_summary')
def get_ontology_summary():
    """Get ontology summary asynchronously"""
    try:
        TASK_COUNT.labels(task_name='get_ontology_summary', status='started').inc()
        with TASK_LATENCY.labels(task_name='get_ontology_summary').time():
            summary = ontology_store.get_ontology_summary()
        TASK_COUNT.labels(task_name='get_ontology_summary', status='success').inc()
        return {"success": True, "summary": summary}
    except Exception as e:
        logger.error(f"Error getting ontology summary: {e}")
        TASK_COUNT.labels(task_name='get_ontology_summary', status='error').inc()
        return {"success": False, "error": str(e)}

@celery_app.task
def update_worker_metrics():
    """Update worker metrics periodically"""
    try:
        i = celery_app.control.inspect()
        active = i.active()
        reserved = i.reserved()
        
        if active and reserved:
            total_pending = sum(len(tasks) for worker_tasks in reserved.values())
            WORKER_TASKS_PENDING.set(total_pending)
    except Exception as e:
        logger.error(f"Error updating worker metrics: {e}")

# Schedule periodic metric updates
celery_app.conf.beat_schedule = {
    'update-worker-metrics': {
        'task': 'tasks.update_worker_metrics',
        'schedule': 60.0,  # Run every minute
    },
}

# --- Master-Slave Workflow Agent Tasks ---

@celery_app.task(name='nlp.query_refinement')
def nlp_query_refinement(parameters):
    """Refine a query using the query refinement agent"""
    adapter = AgentAdapter(QueryRefinementAgent(), "query_refinement")
    return adapter.execute_task(parameters)

@celery_app.task(name='nlp.entity_recognition')
def nlp_entity_recognition(parameters):
    """Recognize entities in a query"""
    adapter = AgentAdapter(EntityRecognitionAgent(), "entity_recognition")
    return adapter.execute_task(parameters)

@celery_app.task(name='query.ontology_mapping')
def query_ontology_mapping(parameters):
    """Map entities to ontology using the ontology mapping agent"""
    adapter = AgentAdapter(OntologyMappingAgent(ontology_store=ontology_store), "ontology_mapping")
    return adapter.execute_task(parameters)

@celery_app.task(name='query.sparql_construction')
def query_sparql_construction(parameters):
    """Construct SPARQL query using the sparql construction agent"""
    adapter = AgentAdapter(SPARQLConstructionAgent(), "sparql_construction")
    return adapter.execute_task(parameters)

@celery_app.task(name='query.validation')
def query_validation(parameters):
    """Validate SPARQL query using the sparql validation agent"""
    adapter = AgentAdapter(SparqlValidationAgent(), "validation")
    return adapter.execute_task(parameters)

@celery_app.task(name='response.query_execution')
def response_query_execution(parameters):
    """Execute SPARQL query using the query execution agent"""
    adapter = AgentAdapter(QueryExecutionAgent(), "query_execution")
    return adapter.execute_task(parameters)

@celery_app.task(name='response.response_generation')
def response_response_generation(parameters):
    """Generate response using the response generation agent"""
    adapter = AgentAdapter(ResponseGenerationAgent(), "response_generation")
    return adapter.execute_task(parameters)