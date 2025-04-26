import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import pytest

from agents.sparql_construction import SPARQLConstructionAgent
from tests.unit.test_utils import create_mock_workflow


class TestSPARQLConstructionAgent(unittest.TestCase):
    """Test cases for the SPARQLConstructionAgent class."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Create a mock LLM client
        self.mock_llm = MagicMock()
        
        # Create a mock ontology client
        self.mock_ontology_client = MagicMock()
        
        # Create the agent with mocks
        self.agent = SPARQLConstructionAgent(
            llm_client=self.mock_llm,
            ontology_client=self.mock_ontology_client
        )
        
        # Create test data
        self.input_data = {
            "query": "Who are professors in the Computer Science department?",
            "ontology_mappings": {
                "professors": "academicStaff",
                "Computer Science": "ComputerScienceDepartment"
            }
        }
        
        # Mock ontology schema
        self.mock_ontology_schema = {
            "classes": ["Professor", "Department", "ComputerScienceDepartment"],
            "properties": ["name", "worksIn", "teachesSubject"],
            "relationships": [
                {"domain": "Professor", "property": "worksIn", "range": "Department"}
            ]
        }
        
    def test_construct_sparql_query(self):
        """Test SPARQL query construction from ontology mappings."""
        # Setup mock ontology client to return schema
        self.mock_ontology_client.get_ontology_schema.return_value = self.mock_ontology_schema
        
        # Setup LLM mock to return a SPARQL query
        mock_sparql = """
        PREFIX uni: <http://www.example.org/university#>
        SELECT ?professor ?name WHERE {
            ?professor a uni:Professor ;
                      uni:worksIn uni:ComputerScienceDepartment ;
                      uni:name ?name .
        }
        """
        self.mock_llm.generate_content.return_value.text = f"""
        {{
            "sparql_query": "{mock_sparql.strip()}"
        }}
        """
        
        # Call the process method
        result = self.agent.process(self.input_data)
        
        # Verify ontology client was called
        self.mock_ontology_client.get_ontology_schema.assert_called_once()
        
        # Verify LLM was called
        self.mock_llm.generate_content.assert_called_once()
        
        # Assert that result contains SPARQL query
        self.assertIn("sparql_query", result)
        self.assertTrue(result["sparql_query"].strip().startswith("PREFIX"))
        
    def test_handle_missing_mappings(self):
        """Test the agent's behavior with missing ontology mappings."""
        # Input data without mappings
        input_without_mappings = {
            "query": "Who are professors in the Computer Science department?"
        }
        
        # Call the process method without mappings
        result = self.agent.process(input_without_mappings)
        
        # Assert that result indicates an error
        self.assertIn("error", result)
        self.assertIn("ontology mappings", result["error"].lower())
        
    def test_validate_sparql_query(self):
        """Test that the agent validates the constructed SPARQL query."""
        # Setup mock ontology client
        self.mock_ontology_client.get_ontology_schema.return_value = self.mock_ontology_schema
        
        # Setup LLM to return invalid SPARQL (missing closing brace)
        invalid_sparql = """
        PREFIX uni: <http://www.example.org/university#>
        SELECT ?professor ?name WHERE {
            ?professor a uni:Professor ;
                      uni:worksIn uni:ComputerScienceDepartment ;
                      uni:name ?name 
        # Note: missing closing brace
        """
        self.mock_llm.generate_content.return_value.text = f"""
        {{
            "sparql_query": "{invalid_sparql.strip()}"
        }}
        """
        
        # Setup a mock validator that detects the error
        with patch('agents.sparql_construction.validate_sparql', return_value=(False, "Missing closing brace")):
            result = self.agent.process(self.input_data)
            
            # Assert that result indicates an error
            self.assertIn("error", result)
            self.assertIn("validation", result["error"].lower())


if __name__ == "__main__":
    unittest.main()