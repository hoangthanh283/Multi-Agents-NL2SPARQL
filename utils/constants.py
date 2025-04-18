import os
from enum import Enum


class QdrantCollections(Enum):
    QUERY_PATTERNS = "query_patterns"
    SPARQL_EXAMPLES = "sparql_examples"
    CONVERSATION_HISTORY = "conversation_history"
    REFINEMENT_EXAMPLES = "refinement_examples"
    ONTOLOGY_EMBEDDING = "ontology_embedding"


VECTOR_SIMILARITY_THRESHOLD = os.getenv("VECTOR_SIMILARITY_THRESHOLD", 0.25)
TOP_K_DRANT_QUERIES = os.getenv("TOP_K_DRANT_QUERIES", 2)
QDRANT_SEARCH_THRESHOLD = os.getenv("QDRANT_SEARCH_THRESHOLD", 0.5)
