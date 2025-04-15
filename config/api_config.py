import os
from typing import Any, Dict

# SPARQL Endpoint Configuration
SPARQL_CONFIG = {
    "default_endpoint": os.getenv("SPARQL_ENDPOINT", "http://localhost:3030/academic/query"),
    "timeout": 30,  # Timeout in seconds
    "auth_token": os.getenv("SPARQL_AUTH_TOKEN"),
    "default_graph": os.getenv("SPARQL_DEFAULT_GRAPH"),
    "result_limit": 1000,  # Default limit for query results
}

# Elasticsearch Configuration
ELASTICSEARCH_CONFIG = {
    "url": os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
    "indices": {
        "TOKEN": "ontology_classes",
        "PROPERTY": "ontology_properties",
        "INSTANCE": "ontology_instances",
        "LITERAL": "ontology_literals"
    },
    "search_limit": 20,  # Default limit for search results
}

# Qdrant Configuration
QDRANT_CONFIG = {
    "url": os.getenv("QDRANT_URL", "http://localhost:6333"),
    "collections": {
        "query_patterns": {
            "vector_size": 384,
            "distance": "Cosine"
        },
        "sparql_examples": {
            "vector_size": 384,
            "distance": "Cosine"
        },
        "conversation_history": {
            "vector_size": 384,
            "distance": "Cosine"
        }
    },
    "search_limit": 5  # Default limit for vector search results
}

# Ontology Configuration
ONTOLOGY_CONFIG = {
    "local_path": os.getenv("ONTOLOGY_PATH", "data/ontologies/academic_ontology.ttl"),
    "remote_endpoint": os.getenv("ONTOLOGY_ENDPOINT"),
    "prefixes": {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "acad": "http://example.org/academic/"
    }
}

# Model Configuration
MODEL_CONFIG = {
    "embedding": {
        "bi_encoder": "sentence-transformers/all-MiniLM-L6-v2",
        "cross_encoder": "cross-encoder/ms-marco-MiniLM-L-6-v2",
        "llm_embedder": "BAAI/bge-large-en-v1.5"
    },
    "entity_recognition": {
        "model": "microsoft/gliner",
        "confidence_threshold": 0.6
    }
}

def get_api_config(config_type: str) -> Dict[str, Any]:
    """Retrieve configuration for a specific API or service."""
    configs = {
        "sparql": SPARQL_CONFIG,
        "elasticsearch": ELASTICSEARCH_CONFIG,
        "qdrant": QDRANT_CONFIG,
        "ontology": ONTOLOGY_CONFIG,
        "model": MODEL_CONFIG
    }
    
    if config_type not in configs:
        raise ValueError(f"Unknown config type: {config_type}")    
    return configs[config_type]
