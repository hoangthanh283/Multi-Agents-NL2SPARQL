import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from database.ontology_store import OntologyStore

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from agents.entity_recognition import EntityRecognitionAgent
from agents.ontology_mapping import OntologyMappingAgent
from agents.query_execution import QueryExecutionAgent
from agents.response_generation import ResponseGenerationAgent
from agents.sparql_construction import SPARQLConstructionAgent
from utils.constants import CHEVIE_ONTOLOGY_PATH


class TestNL2SPARQLPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Use real ontology store with CHeVIE_comment.owl
        self.ontology_store = OntologyStore(local_path=CHEVIE_ONTOLOGY_PATH)
        loaded = self.ontology_store.load_ontology()
        if not loaded:
            raise RuntimeError(f"Failed to load ontology from {CHEVIE_ONTOLOGY_PATH}")
        
        # Create agents with real ontology store
        with patch('autogen.AssistantAgent'), patch('autogen.UserProxyAgent'):
            self.entity_agent = EntityRecognitionAgent(ontology_store=self.ontology_store)
            self.mapping_agent = OntologyMappingAgent(ontology_store=self.ontology_store)
            self.sparql_agent = SPARQLConstructionAgent(ontology_store=self.ontology_store)
            self.execution_agent = QueryExecutionAgent(ontology_store=self.ontology_store)
            self.response_agent = ResponseGenerationAgent()
        
        # Example natural language query
        self.nl_query = "List all professors who teach Computer Science courses"
        
        # Mock query execution results
        execution_patcher = patch.object(
            self.execution_agent, 'execute_query',
            return_value={
                "success": True,
                "results": [
                    {"professor": "http://example.org/Professor1", "name": "John Smith"},
                    {"professor": "http://example.org/Professor2", "name": "Jane Doe"}
                ],
                "headers": ["professor", "name"],
                "execution_time": 0.15
            }
        )
        self.mock_executor = execution_patcher.start()
        self.addCleanup(execution_patcher.stop)
    
    def test_full_nl2sparql_pipeline(self):
        """Test the complete NL-to-SPARQL pipeline flow"""
        # Step 1: Entity Recognition
        entities_result = self.entity_agent.recognize_entities(self.nl_query)
        self.assertIsNotNone(entities_result)
        self.assertIn("entities", entities_result)
        
        # Step 2: Ontology Mapping
        mapping_result = self.mapping_agent.map_entities(self.nl_query, entities_result["entities"])
        self.assertIsNotNone(mapping_result)
        
        # Verify mapping structure
        self.assertIn("classes", mapping_result)
        self.assertIn("properties", mapping_result)
        self.assertIn("individuals", mapping_result)
        
        # Step 3: SPARQL Construction
        sparql_result = self.sparql_agent.construct_query(self.nl_query, mapping_result)
        self.assertIsNotNone(sparql_result)
        self.assertIn("sparql", sparql_result)
        self.assertIn("query_type", sparql_result)
        
        # Verify SPARQL query
        self.assertIn("SELECT", sparql_result["sparql"])
        self.assertIn("WHERE", sparql_result["sparql"])
        
        # Step 4: Query Execution
        execution_result = self.execution_agent.execute_query(sparql_result["sparql"])
        self.assertIsNotNone(execution_result)
        self.assertTrue(execution_result["success"])
        self.assertIn("results", execution_result)
        
        # Step 5: Response Generation
        response_result = self.response_agent.generate_response(
            self.nl_query, 
            sparql_result["sparql"], 
            execution_result
        )
        self.assertIsNotNone(response_result)
        self.assertIn("response", response_result)
        self.assertIn("format", response_result)
        
    def test_error_handling_in_pipeline(self):
        """Test error handling in the pipeline"""
        # Mock entity recognition to throw an error
        with patch.object(
            self.entity_agent, 'recognize_entities', 
            side_effect=Exception("Entity recognition failed")
        ):
            # Should raise the exception
            with self.assertRaises(Exception):
                self.entity_agent.recognize_entities(self.nl_query)
        
        # Mock query execution to return an error
        with patch.object(
            self.execution_agent, 'execute_query',
            return_value={
                "success": False,
                "error": "Invalid SPARQL syntax",
                "details": "Unexpected token in query"
            }
        ):
            invalid_query = "SELECT * WHERE { INVALID QUERY }"
            execution_result = self.execution_agent.execute_query(invalid_query)
            self.assertFalse(execution_result["success"])
            self.assertIn("error", execution_result)
            
            # Test response generation with error
            response_result = self.response_agent.generate_response(
                self.nl_query, 
                invalid_query, 
                execution_result
            )
            self.assertIn("response", response_result)
            self.assertIn("error", response_result["response"].lower())

if __name__ == "__main__":
    unittest.main()