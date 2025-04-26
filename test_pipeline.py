import sys
import time
from celery import Celery

# Import the Celery app from tasks.py
from tasks import celery_app

# Example test queries
TEST_SPARQL = "SELECT ?s WHERE { ?s a ?type } LIMIT 1"
TEST_CLASS_QUERY = "Person"


def test_execute_sparql():
    print("Testing execute_sparql...")
    result = celery_app.send_task('execute_sparql', args=[TEST_SPARQL])
    print("Task submitted. Waiting for result...")
    output = result.get(timeout=30)
    print("Result:", output)


def test_search_classes():
    print("Testing search_classes...")
    result = celery_app.send_task('search_classes', args=[TEST_CLASS_QUERY])
    print("Task submitted. Waiting for result...")
    output = result.get(timeout=30)
    print("Result:", output)


def test_search_properties():
    print("Testing search_properties...")
    result = celery_app.send_task('search_properties', args=[TEST_CLASS_QUERY])
    print("Task submitted. Waiting for result...")
    output = result.get(timeout=30)
    print("Result:", output)


def test_search_instances():
    print("Testing search_instances...")
    result = celery_app.send_task('search_instances', args=[TEST_CLASS_QUERY])
    print("Task submitted. Waiting for result...")
    output = result.get(timeout=30)
    print("Result:", output)


def test_get_ontology_summary():
    print("Testing get_ontology_summary...")
    result = celery_app.send_task('get_ontology_summary')
    print("Task submitted. Waiting for result...")
    output = result.get(timeout=30)
    print("Result:", output)


def main():
    print("--- E2E Pipeline Test ---")
    test_execute_sparql()
    test_search_classes()
    test_search_properties()
    test_search_instances()
    test_get_ontology_summary()
    print("--- All tests completed ---")

if __name__ == "__main__":
    main()
