import os
from enum import Enum

from database.qdrant_client import QdrantClient


class QdrantCollections(Enum):
    QUERY_PATTERNS = "query_patterns"
    SPARQL_EXAMPLES = "sparql_examples"
    CONVERSATION_HISTORY = "conversation_history"
    REFINEMENT_EXAMPLES = "refinement_examples"
    ONTOLOGY_EMBEDDING = "ontology_embedding"


VECTOR_SIMILARITY_THRESHOLD = 0.7
TOP_K_DRANT_QUERIES = os.getenv("TOP_K_DRANT_QUERIES", 2)
QDRANT_SEARCH_THRESHOLD = os.getenv("QDRANT_SEARCH_THRESHOLD", 0.5)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHEVIE_ONTOLOGY_PATH = os.path.join(PROJECT_ROOT, "data", "ontologies", "CHeVIE_comment.owl")

# Create singleton QdrantClient instance
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_CLIENT_SINGLETON = QdrantClient(url=QDRANT_URL)
