import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from database.ontology_store import OntologyStore

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from agents.sparql_construction import SPARQLConstructionAgent
from tests.unit.test_utils import create_mock_workflow
from utils.constants import CHEVIE_ONTOLOGY_PATH


class TestSPARQLConstructionAgent(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Mock template directory
        self.temp_templates_dir = "/tmp/test_sparql_templates"
        if not os.path.exists(self.temp_templates_dir):
            os.makedirs(self.temp_templates_dir)
        
        # Use real ontology store with CHeVIE_comment.owl
        self.ontology_store = OntologyStore(local_path=CHEVIE_ONTOLOGY_PATH)
        loaded = self.ontology_store.load_ontology()
        if not loaded:
            raise RuntimeError(f"Failed to load ontology from {CHEVIE_ONTOLOGY_PATH}")
        
        # Create a mock agent with template directory and ontology store
        with patch('autogen.AssistantAgent'), patch('autogen.UserProxyAgent'):
            self.agent = SPARQLConstructionAgent(templates_dir=self.temp_templates_dir, ontology_store=self.ontology_store)
        
        # Create example templates for testing
        self.agent._create_example_templates()
        
        # Get mock workflow data
        self.mock_workflow = create_mock_workflow()
        
    def tearDown(self):
        """Clean up after each test"""
        # Remove test template files
        if os.path.exists(self.temp_templates_dir):
            for file in os.listdir(self.temp_templates_dir):
                os.remove(os.path.join(self.temp_templates_dir, file))
            os.rmdir(self.temp_templates_dir)
    
    def test_template_loading(self):
        """Test that templates are correctly loaded"""
        templates = self.agent._load_templates()
        self.assertTrue(len(templates) >= 5)  # We expect at least our example templates
        
        # Check that required fields are present in templates
        for template in templates:
            self.assertIn("id", template)
            self.assertIn("pattern", template)
            self.assertIn("query_type", template)
    
    def test_determine_query_type(self):
        """Test query type determination"""
        # Test ASK query detection
        ask_query = "Is there a professor with name John?"
        query_type = self.agent._determine_query_type(ask_query, {})
        self.assertEqual(query_type, "ASK")
        
        # Test SELECT query detection (default)
        select_query = "List all professors in Computer Science"
        query_type = self.agent._determine_query_type(select_query, {})
        self.assertEqual(query_type, "SELECT")
    
    def test_construct_query_with_template(self):
        """Test query construction using a template"""
        # Use mapped entities that match the 'class_instances' template
        mapped_entities = {
            "classes": [
                {
                    "text": "Professor",
                    "uri": "http://example.org/ontology#Professor",
                    "confidence": 0.9
                }
            ]
        }
        
        refined_query = "List all professors"
        result = self.agent.construct_query(refined_query, mapped_entities)
        
        self.assertIn("sparql", result)
        self.assertIn("PREFIX", result["sparql"])
        self.assertIn("SELECT", result["sparql"])
        self.assertIn("http://example.org/ontology#Professor", result["sparql"])
        self.assertEqual(result["query_type"], "SELECT")
        self.assertTrue(result["template_based"])
    
    def test_construct_query_with_llm(self):
        """Test query construction falling back to LLM"""
        # Mock the _llm_based_construction method
        with patch.object(self.agent, '_llm_based_construction') as mock_llm:
            mock_llm.return_value = {
                "sparql": "SELECT ?x WHERE { ?x a <http://example.org/ontology#CustomClass> }",
                "query_type": "SELECT",
                "template_based": False
            }
            
            # Use entities that won't match any template
            mapped_entities = {
                "custom_type": [
                    {
                        "text": "CustomClass",
                        "uri": "http://example.org/ontology#CustomClass",
                        "confidence": 0.9
                    }
                ]
            }
            
            refined_query = "Show me all instances of CustomClass"
            result = self.agent.construct_query(refined_query, mapped_entities)
            
            # Check that LLM-based construction was called
            mock_llm.assert_called_once()
            self.assertFalse(result["template_based"])
    
    def test_add_prefixes(self):
        """Test adding prefixes to queries"""
        query = "SELECT ?instance WHERE { ?instance a <http://example.org/ontology#Professor> }"
        mapped_entities = {
            "classes": [
                {
                    "text": "Professor",
                    "uri": "http://example.org/ontology#Professor",
                    "confidence": 0.9
                }
            ]
        }
        
        prefixed_query = self.agent._add_prefixes(query, mapped_entities)
        
        self.assertIn("PREFIX rdf:", prefixed_query)
        self.assertIn("PREFIX rdfs:", prefixed_query)
        self.assertIn("PREFIX owl:", prefixed_query)
        self.assertIn("PREFIX ex:", prefixed_query)  # Should add prefix for example.org
        
    def test_query_construction_with_real_workflow_data(self):
        """Test query construction with real workflow data from test_utils"""
        refined_query = self.mock_workflow["refined_query"]
        mapped_entities = self.mock_workflow["mapped_entities"]
        
        result = self.agent.construct_query(refined_query, mapped_entities)
        
        self.assertIn("sparql", result)
        self.assertIn("query_type", result)
        self.assertIn("entities_used", result)
        
        # Verify correct entity usage
        for entity_type in mapped_entities.keys():
            if mapped_entities[entity_type]:
                # At least one entity of each populated type should be used
                entity_uris = [e.get("uri") for e in mapped_entities[entity_type] if "uri" in e]
                if entity_uris:
                    found = False
                    for used_entity in result["entities_used"]:
                        if used_entity.get("uri") in entity_uris:
                            found = True
                            break
                    self.assertTrue(found, f"No entity of type {entity_type} was used in the query")

if __name__ == "__main__":
    unittest.main()