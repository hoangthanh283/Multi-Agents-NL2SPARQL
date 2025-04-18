import os


QDRANT_COLLECTIONS = frozenset(["query_patterns", "sparql_examples", "conversation_history", "refinement_examples", "ontology_embedding"])
VECTOR_SIMILARITY_THRESHOLD = os.getenv("VECTOR_SIMILARITY_THRESHOLD", 0.25)
TOP_K_DRANT_QUERIES = os.getenv("TOP_K_DRANT_QUERIES", 2)
QDRANT_SEARCH_THRESHOLD = os.getenv("QDRANT_SEARCH_THRESHOLD", 0.5)