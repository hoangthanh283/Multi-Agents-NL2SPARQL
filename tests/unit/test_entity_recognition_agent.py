import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from database.ontology_store import OntologyStore
from utils.constants import CHEVIE_ONTOLOGY_PATH

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from agents.entity_recognition import EntityRecognitionAgent
from tests.unit.test_utils import create_mock_workflow


class TestEntityRecognitionAgent(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create mock entity recognition model
        self.mock_model = MagicMock()
        self.mock_model.extract_entities.return_value = [
            {
                "entity_text": "professors",
                "entity_type": "CLASS",
                "start_position": 0,
                "end_position": 10,
                "confidence": 0.95
            },
            {
                "entity_text": "Computer Science",
                "entity_type": "INSTANCE",
                "start_position": 18,
                "end_position": 34,
                "confidence": 0.92
            }
        ]
        
        # Use real ontology store with CHeVIE_comment.owl
        self.ontology_store = OntologyStore(local_path=CHEVIE_ONTOLOGY_PATH)
        loaded = self.ontology_store.load_ontology()
        if not loaded:
            raise RuntimeError(f"Failed to load ontology from {CHEVIE_ONTOLOGY_PATH}")
        
        # Create the agent
        self.agent = EntityRecognitionAgent(
            entity_recognition_model=self.mock_model,
            ontology_store=self.ontology_store
        )
        
        # Get a mock workflow for testing
        self.mock_workflow = create_mock_workflow()
    
    def test_recognize_entities(self):
        """Test the recognize_entities method"""
        query = "List professors working in the Computer Science department"
        result = self.agent.recognize_entities(query)
        
        # Assert that the model was called correctly
        self.mock_model.extract_entities.assert_called_once_with(
            text=query,
            entity_types=self.agent.entity_types
        )
        
        # Check that result contains the expected structure
        self.assertIn("class", result)
        self.assertIn("instance", result)
        self.assertIn("all_entities", result)
        
        # Check that entities were enriched with ontology information
        self.assertTrue(any("ontology_matches" in entity for entity in result["class"]))
        self.assertTrue(any("ontology_matches" in entity for entity in result["instance"]))
    
    def test_rule_based_extraction(self):
        """Test rule-based extraction for entities"""
        query = "Show professors with age greater than 40"
        result = self.agent.recognize_entities(query)
        
        # Check that QUERY_TYPE was detected based on "Show"
        query_types = [entity["text"] for entity in result.get("query_type", [])]
        self.assertIn("SELECT", query_types)
        
        # Check that FILTER was detected based on "greater than"
        filters = [entity["text"] for entity in result.get("filter", [])]
        self.assertIn("greater_than", filters)
        
        # Check that the number was recognized as a literal
        literals = [entity["text"] for entity in result.get("literal", [])]
        self.assertIn("40", literals)
    
    def test_empty_query(self):
        """Test handling of empty queries"""
        result = self.agent.recognize_entities("")
        self.assertEqual(len(result["all_entities"]), 0)
        
    def test_clean_entity_text(self):
        """Test the _clean_entity_text method"""
        # Test removing stopwords
        self.assertEqual(self.agent._clean_entity_text("the professor"), "professor")
        self.assertEqual(self.agent._clean_entity_text("professor of"), "professor")
        self.assertEqual(self.agent._clean_entity_text("the professor of"), "professor")
        
        # Test handling whitespace
        self.assertEqual(self.agent._clean_entity_text("  professor  "), "professor")

if __name__ == "__main__":
    unittest.main()