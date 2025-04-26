import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

import pytest

from agents.entity_recognition import EntityRecognitionAgent
from tests.unit.test_utils import create_mock_workflow


class TestEntityRecognitionAgent(unittest.TestCase):
    """Test cases for the EntityRecognitionAgent class."""
    
    def setUp(self):
        """Set up test fixtures before each test."""
        # Create a mock LLM client
        self.mock_llm = MagicMock()
        
        # Create the agent with the mock LLM
        self.agent = EntityRecognitionAgent(llm_client=self.mock_llm)
        
        # Create a mock workflow
        self.workflow = create_mock_workflow()
        
    def test_extract_entities(self):
        """Test that the agent can extract entities from a query."""
        # Setup LLM mock to return some entities
        self.mock_llm.generate_content.return_value.text = """
        {
            "entities": [
                {"text": "professors", "type": "ROLE"},
                {"text": "Computer Science", "type": "DEPARTMENT"}
            ]
        }
        """
        
        # Call the process method with a query
        result = self.agent.process({
            "query": "Who are professors in the Computer Science department?"
        })
        
        # Assert that the mock was called with the right prompt
        self.mock_llm.generate_content.assert_called_once()
        
        # Assert the result contains entities
        self.assertIn("entities", result)
        self.assertEqual(len(result["entities"]), 2)
        
        # Check first entity
        self.assertEqual(result["entities"][0]["text"], "professors")
        self.assertEqual(result["entities"][0]["type"], "ROLE")
        
        # Check second entity
        self.assertEqual(result["entities"][1]["text"], "Computer Science")
        self.assertEqual(result["entities"][1]["type"], "DEPARTMENT")
        
    def test_handle_empty_query(self):
        """Test the agent's behavior with an empty query."""
        # Call the process method with an empty query
        result = self.agent.process({"query": ""})
        
        # Assert that result indicates an error
        self.assertIn("error", result)
        self.assertIn("empty query", result["error"].lower())
        
    def test_handle_llm_error(self):
        """Test the agent's behavior when the LLM fails."""
        # Setup LLM mock to raise an exception
        self.mock_llm.generate_content.side_effect = Exception("LLM service unavailable")
        
        # Call the process method
        result = self.agent.process({
            "query": "Who are professors in the Computer Science department?"
        })
        
        # Assert that result indicates an error
        self.assertIn("error", result)
        self.assertIn("llm", result["error"].lower())


if __name__ == "__main__":
    unittest.main()