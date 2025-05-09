import os
from typing import Any, Dict

# ---------------------------------------------------------------------------
# 1. Keys / endpoints from the host environment
#    (all look-ups are now inlined below)
# ---------------------------------------------------------------------------

# Choose provider: "openai" | "azure" | "ollama"         (defaults to ollama)
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()

# ---------------------------------------------------------------------------
# 2. Build the provider-specific config_list
# ---------------------------------------------------------------------------
if LLM_PROVIDER == "openai":
    _CONFIG_LIST = [
        {
            "model": os.getenv("OPENAI_MODEL"),
            "api_key": os.getenv("OPENAI_API_KEY"),
            # No base_url → platform default (api.openai.com)
        }
    ]

elif LLM_PROVIDER == "azure":
    _CONFIG_LIST = [
        {
            "model": os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
            "api_key": os.getenv("AZURE_OPENAI_API_KEY"),
            "base_url": os.getenv("AZURE_OPENAI_ENDPOINT"),
            "api_type": "azure",
            "api_version": os.getenv("AZURE_OPENAI_API_VERSION"),
        }
    ]

elif LLM_PROVIDER == "ollama":
    _CONFIG_LIST = [
        {
            "model": os.getenv("OLLAMA_MODEL"),
            "base_url": os.getenv("OLLAMA_BASE_URL"),
            # Ollama normally ignores api_key; keep it for drop-in compatibility
            "api_key": os.getenv("OPENAI_API_KEY"),
        }
    ]

else:
    raise ValueError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")

# ---------------------------------------------------------------------------
# 3. Unified LLM_CONFIG
# ---------------------------------------------------------------------------
LLM_CONFIG: Dict[str, Any] = {
    "config_list": _CONFIG_LIST,
    "temperature": 0.0,
    "timeout": 600,
}

# Configuration for each agent type
AGENT_CONFIGS = {
    "master": {
        "name": "MasterAgent",
        "description": "Coordinates all slave agents and manages the overall conversation flow",
        "llm_config": {**LLM_CONFIG, "temperature": 0.0},
        "system_message": """You are the master coordinator for a Natural Language to SPARQL conversion system.
Your job is to coordinate between specialized slave agents to convert natural language queries into 
accurate SPARQL queries for querying knowledge graphs. Analyze user queries, delegate tasks
to appropriate slave agents, evaluate their outputs, and synthesize final responses."""
    },
    "query_refinement": {
        "name": "QueryRefinementAgent",
        "description": "Refines user queries into clear, standalone questions",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """You specialize in understanding and refining user queries about knowledge graphs.
Transform vague, ambiguous, or context-dependent questions into clear, standalone questions that are
suitable for conversion to SPARQL. Use conversation history to fill in missing details.
Your output should be a refined query that captures the semantic intent clearly."""
    },
    "entity_recognition": {
        "name": "EntityRecognitionAgent",
        "description": "Identifies knowledge graph entities in user queries",
        "llm_config": None,  # This agent uses a custom model, not an LLM
        "system_message": "Identify entities like classes, properties, instances, and literals in queries."
    },
    "ontology_mapping": {
        "name": "OntologyMappingAgent",
        "description": "Maps natural language entities to ontology terms",
        "llm_config": {**LLM_CONFIG, "temperature": 0.0},
        "system_message": """You are an ontology mapping specialist. Your task is to map natural 
language terms to formal ontology terms. Analyze the context, term descriptions, and 
ontology structure to find the best matches between user language and formal knowledge graph concepts."""
    },
    "tool_selection": {
        "name": "ToolSelectionAgent",
        "description": "Selects appropriate SPARQL templates",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """Analyze refined queries to determine which SPARQL templates are needed.
Return a JSON list of relevant templates with their priority scores."""
    },
    "plan_formulation": {
        "name": "PlanFormulationAgent", 
        "description": "Creates execution plans for SPARQL queries",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """Create detailed execution plans for SPARQL queries.
Specify parameter values and establish dependencies between query steps if needed.
Return a structured JSON execution plan."""
    },
    "validation": {
        "name": "ValidationAgent",
        "description": "Validates execution plans for errors",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """Check execution plans for logical errors or hallucinations.
Verify entity-parameter mappings for accuracy. Return validation results with pass/fail status
and suggested corrections."""
    },
    "sparql_construction": {
        "name": "SPARQLConstructionAgent",
        "description": "Creates SPARQL queries from mapped entities",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """You specialize in constructing valid SPARQL queries from mapped ontology entities.
Create syntactically correct SPARQL that accurately captures the user's intent using the 
appropriate query form (SELECT, ASK, CONSTRUCT, DESCRIBE) and query patterns."""
    },
    "sparql_validation": {
        "name": "SPARQLValidationAgent",
        "description": "Validates SPARQL queries for correctness",
        "llm_config": {**LLM_CONFIG, "temperature": 0},
        "system_message": """You specialize in validating SPARQL queries for syntax and semantic correctness.
Check queries for syntax errors, logical issues, and potential performance problems.
Your goal is to ensure queries will execute correctly against a SPARQL endpoint."""
    },
    "query_execution": {
        "name": "QueryExecutionAgent",
        "description": "Executes SPARQL queries against endpoints",
        "llm_config": None,  # This agent uses direct API calls, not an LLM
        "system_message": "Execute SPARQL queries against knowledge graph endpoints."
    },
    "tool_execution": {
        "name": "ToolExecutionAgent",
        "description": "Executes SPARQL queries via a wrapper interface",
        "llm_config": None,  # This agent uses direct API calls, not an LLM
        "system_message": "Execute SPARQL queries against knowledge graph endpoints using a tool interface."
    },
    "response_generation": {
        "name": "ResponseGenerationAgent",
        "description": "Generates natural language responses from SPARQL results",
        "llm_config": {**LLM_CONFIG, "temperature": 0.0},
        "system_message": """Transform SPARQL query results into natural language responses.
Explain the results clearly to users who may not understand SPARQL or knowledge graph structure.
Format information in a user-friendly way and provide context for the query results."""
    }
}

# Function to get agent config by type
def get_agent_config(agent_type: str) -> Dict[str, Any]:
    """Retrieve configuration for a specific agent type."""
    if agent_type not in AGENT_CONFIGS:
        raise ValueError(f"Unknown agent type: {agent_type}")
    return AGENT_CONFIGS[agent_type]
