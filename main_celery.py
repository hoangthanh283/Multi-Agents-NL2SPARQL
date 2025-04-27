import os
from celery import Celery

# Set up Celery app (reuse config from tasks.py)
celery_app = Celery('nl2sparql_tasks',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/1'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/2')
)

# Example user query for E2E test
USER_QUERY = "What are the subclasses of Person?"


def call_task(task_name, parameters=None, timeout=60):
    print(f"Calling Celery task: {task_name}")
    result = celery_app.send_task(task_name, args=[parameters] if parameters is not None else [])
    output = result.get(timeout=timeout)
    print(f"Result from {task_name}:\n", output, "\n")
    return output


def main():
    print("--- NL2SPARQL Celery E2E Pipeline ---")
    # Step 1: Query Refinement
    refined = call_task('nlp.query_refinement', {"raw_query": USER_QUERY, "conversation_history": []})
    refined_query = refined.get('result', {}).get('refined_query', USER_QUERY)

    # Step 2: Entity Recognition
    entities = call_task('nlp.entity_recognition', {"query": refined_query})

    # Step 3: Ontology Mapping
    mapped = call_task('query.ontology_mapping', {"entities": entities.get('result', {}).get('entities', []), "query_context": refined_query})

    # Step 4: Plan Formulation
    plan = call_task('response.plan_formulation', {"refined_query": refined_query, "mapped_entities": mapped.get('result', {}).get('mapped_entities', {})})

    # Step 6: Validation (use plan result)
    validation = call_task('query.validation', plan.get('result', {}))

    # Step 5: SPARQL Construction (use validation result)
    sparql = call_task('query.sparql_construction', validation.get('result', {}))
    sparql_query = sparql.get('result', {}).get('sparql_query', '')

    # Step 7: Query Execution
    execution = call_task('response.query_execution', {"sparql_query": sparql_query})

    # Step 8: Response Generation
    response = call_task('response.response_generation', {"query_results": execution.get('result', {}), "original_query": USER_QUERY})

    print("--- Pipeline Complete ---")
    print("Final Response:", response)


if __name__ == "__main__":
    main()
