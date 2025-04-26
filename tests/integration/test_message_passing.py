import os
import sys
import time
import json
import threading
import logging
from typing import Dict, Any

import redis
import pytest

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from master.global_master import GlobalMaster
from slaves.slave_pool_manager import SlavePoolManager
from utils.logging_utils import setup_logging

# Setup logging
logger = setup_logging(app_name="nl-to-sparql-test", enable_colors=True)
logger.setLevel(logging.INFO)

# Redis URL for testing
REDIS_URL = os.environ.get("REDIS_URL_TEST", "redis://localhost:6379/1")

class TestMessagePassing:
    """
    Test class for verifying message passing between components in the master-slave architecture.
    """
    
    @classmethod
    def setup_class(cls):
        """Set up test environment once before all tests."""
        # Clean up Redis before tests
        cls.redis = redis.from_url(REDIS_URL)
        cls.redis.flushdb()
        
        # Configuration for slave pools
        cls.pool_configs = {
            # Use minimal pool sizes for testing
            "nlp.query_refinement": {"initial_size": 1, "max_size": 1},
            "nlp.entity_recognition": {"initial_size": 1, "max_size": 1},
            "query.ontology_mapping": {"initial_size": 1, "max_size": 1},
            "query.sparql_construction": {"initial_size": 1, "max_size": 1},
            "response.query_execution": {"initial_size": 1, "max_size": 1},
            "response.response_generation": {"initial_size": 1, "max_size": 1}
        }
    
    @classmethod
    def teardown_class(cls):
        """Clean up after all tests."""
        # Clean up Redis after tests
        cls.redis.flushdb()
    
    def setup_method(self):
        """Set up before each test."""
        # Initialize components for testing
        self.global_master = GlobalMaster(REDIS_URL)
        self.slave_pool_manager = SlavePoolManager(REDIS_URL, self.pool_configs)
        
        # Start components
        self.global_master.start()
        
        # Start only necessary slave pools for faster testing
        self.slave_pool_manager.start_pools(
            domains=["nlp", "query", "response"],
            slave_types={
                "nlp": ["query_refinement", "entity_recognition"],
                "query": ["ontology_mapping", "sparql_construction"],
                "response": ["query_execution", "response_generation"]
            }
        )
        
        # Allow time for components to start
        time.sleep(2)
    
    def teardown_method(self):
        """Clean up after each test."""
        # Stop components
        self.slave_pool_manager.stop_all_pools()
        self.global_master.stop()
        
        # Allow time for components to stop properly
        time.sleep(1)
        
        # Clean up workflows
        self.redis.flushdb()
    
    def test_nlp_domain_message_passing(self):
        """Test message passing between NLP domain master and slaves."""
        # Create a test workflow
        query = "What are the top 5 research papers in natural language processing?"
        request_id = self.global_master.create_workflow(query)
        
        # Start the workflow (sends message to NLP domain)
        self.global_master.start_workflow(request_id)
        
        # Wait for query refinement and entity recognition to complete
        # In a real test, we would use proper signaling mechanisms
        max_wait_time = 30  # seconds
        start_time = time.time()
        
        # Check workflow status periodically
        while time.time() - start_time < max_wait_time:
            status = self.global_master.get_workflow_status(request_id)
            
            # Check if workflow has moved to query domain
            if status and status.get("current_domain") == "query":
                break
                
            # Wait before checking again
            time.sleep(0.5)
        else:
            pytest.fail(f"Workflow did not progress to query domain within {max_wait_time} seconds")
        
        # Verify workflow status
        status = self.global_master.get_workflow_status(request_id)
        assert status is not None
        assert status.get("current_domain") == "query"
        
        # Verify workflow data
        workflow_key = f"workflow:{request_id}"
        workflow_json = self.redis.get(workflow_key)
        assert workflow_json is not None
        
        workflow = json.loads(workflow_json)
        assert "data" in workflow
        assert "refined_query" in workflow["data"]
        assert "entities" in workflow["data"]
    
    def test_complete_workflow(self):
        """Test a complete workflow from NL query to SPARQL and response."""
        # Create a test workflow with a simple query
        query = "List all professors in the computer science department"
        request_id = self.global_master.create_workflow(query)
        
        # Start the workflow
        self.global_master.start_workflow(request_id)
        
        # Wait for workflow completion
        max_wait_time = 60  # seconds
        start_time = time.time()
        
        # Check workflow status periodically
        while time.time() - start_time < max_wait_time:
            status = self.global_master.get_workflow_status(request_id)
            
            # Check if workflow is completed
            if status and status.get("completed"):
                break
                
            # Wait before checking again
            time.sleep(1)
        else:
            pytest.fail(f"Workflow did not complete within {max_wait_time} seconds")
        
        # Get the final result
        result = self.global_master.get_workflow_result(request_id)
        assert result is not None
        assert result.get("completed") is True
        
        # Check result data
        assert "sparql_query" in result
        assert "response" in result
        
        logger.info(f"Original query: {result.get('original_query')}")
        logger.info(f"SPARQL query: {result.get('sparql_query')}")
        logger.info(f"Response: {result.get('response')}")
    
    def test_error_handling(self):
        """Test error handling in the message passing system."""
        # Create a test workflow with an empty query (should cause errors)
        request_id = self.global_master.create_workflow("")
        
        # Start the workflow
        self.global_master.start_workflow(request_id)
        
        # Wait for workflow completion (with error)
        max_wait_time = 30  # seconds
        start_time = time.time()
        
        # Check workflow status periodically
        while time.time() - start_time < max_wait_time:
            status = self.global_master.get_workflow_status(request_id)
            
            # Check if workflow has an error
            if status and status.get("has_error"):
                break
                
            # Wait before checking again
            time.sleep(0.5)
        else:
            pytest.fail(f"Error handling not triggered within {max_wait_time} seconds")
        
        # Verify error in workflow
        status = self.global_master.get_workflow_status(request_id)
        assert status.get("has_error") is True
        assert "error" in status


if __name__ == "__main__":
    # Run the tests manually
    test = TestMessagePassing()
    test.setup_class()
    
    try:
        test.setup_method()
        print("\n=== Testing NLP domain message passing ===")
        test.test_nlp_domain_message_passing()
        test.teardown_method()
        
        test.setup_method()
        print("\n=== Testing complete workflow ===")
        test.test_complete_workflow()
        test.teardown_method()
        
        test.setup_method()
        print("\n=== Testing error handling ===")
        test.test_error_handling()
        test.teardown_method()
        
    finally:
        test.teardown_class()