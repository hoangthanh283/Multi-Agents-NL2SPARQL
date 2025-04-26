import os
import sys
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

def create_mock_workflow():
    """Create a mock workflow for testing purposes"""
    return {
        "id": "test-workflow-123",
        "status": "in_progress",
        "data": {
            "original_query": "Who are professors in the Computer Science department?",
            "refined_query": "List professors working in the Computer Science department",
            "entities": [
                {"text": "professors", "type": "ROLE"},
                {"text": "Computer Science", "type": "DEPARTMENT"}
            ],
            "ontology_mappings": {
                "professors": "academicStaff",
                "Computer Science": "ComputerScienceDepartment"
            },
            "sparql_query": """
                PREFIX uni: <http://www.example.org/university#>
                SELECT ?professor ?name WHERE {
                    ?professor a uni:Professor ;
                              uni:worksIn uni:ComputerScienceDepartment ;
                              uni:name ?name .
                }
            """,
            "query_results": [
                {"professor": "http://example.org/professor1", "name": "Jane Doe"},
                {"professor": "http://example.org/professor2", "name": "John Smith"}
            ],
            "response": "The professors in the Computer Science department are Jane Doe and John Smith."
        },
        "errors": []
    }

class MockResponse:
    """Mock response object for testing HTTP requests"""
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        
    def json(self):
        return self._json_data