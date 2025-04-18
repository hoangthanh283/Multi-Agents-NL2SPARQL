from enum import Enum


class QdrantCollections(Enum):
    QUERY_PATTERNS = "query_patterns"
    SPARQL_EXAMPLES = "sparql_examples"
    CONVERSATION_HISTORY = "conversation_history"
    REFINEMENT_EXAMPLES = "refinement_examples"
    ONTOLOGY_EMBEDDING = "ontology_embedding"
