# Natural Language to SPARQL Converter

A multi-agent system for converting natural language queries into SPARQL queries for knowledge graph exploration, implemented using a Master-Slave architecture with AutoGen.

## Project Structure
```
NL2SPARQL
│
├── config/                       # Configuration settings
│   ├── __init__.py
│   ├── agent_config.py           # AutoGen agent configurations
│   └── api_config.py             # External API configurations
│
├── agents/                       # Agent implementations
│   ├── __init__.py
│   ├── master_agent.py           # Coordinates the entire workflow
│   ├── query_refinement.py       # Refines ambiguous natural language queries
│   ├── entity_recognition.py     # Extracts ontology-related entities from queries
│   ├── ontology_mapping.py       # Maps entities to formal ontology terms
│   ├── tool_selection.py         # Selects appropriate SPARQL templates
│   ├── plan_formulation.py       # Creates query execution plans
│   ├── validation.py             # Validates plans to prevent hallucinations
│   ├── sparql_construction.py    # Constructs SPARQL queries from plans
│   ├── sparql_validation.py      # Validates SPARQL query syntax and semantics
│   ├── tool_execution.py         # Wrapper for query execution
│   ├── query_execution.py        # Executes SPARQL queries against endpoints
│   └── response_generation.py    # Generates natural language responses
│
├── database/                     # Database connectors
│   ├── __init__.py
│   ├── qdrant_client.py          # Vector database for semantic search
│   ├── elastic_client.py         # Entity resolution and search
│   └── ontology_store.py         # RDF graph management and access
│
├── models/                       # Machine learning models
│   ├── __init__.py
│   ├── embeddings.py             # Embedding models (Bi-encoder, Cross-encoder)
│   └── entity_recognition.py     # GLiNER entity recognition model
│
├── tools/                        # Utility tools
│   ├── __init__.py
│   └── sparql_tools.py           # SPARQL query utilities
│
├── utils/                        # General utilities
│   ├── __init__.py
│   └── logging_utils.py          # Logging configuration and tools
│
├── templates/                    # Query templates
│   └── sparql/                   # SPARQL query templates
│       ├── class_instances.json  # Template for listing class instances
│       ├── instance_properties.json # Template for instance properties
│       ├── property_values.json  # Template for property values
│       ├── instance_exists.json  # Template for checking instance existence
│       └── filtered_instances.json # Template for filtered instances
│
├── assets/                        # Data files
│   └── ontologies/
│       └── academic_ontology.ttl # Sample academic domain ontology
│
├── main.py                       # Application entry point
├── requirements.txt              # Project dependencies
└── README.md                     # Project documentation
```

## Architecture Overview

This system uses a Master-Slave architecture where a central Master Agent coordinates multiple specialized Slave Agents, each responsible for a specific task in the natural language to SPARQL conversion workflow.

### Master Agent

The Master Agent serves as the central coordinator with these responsibilities:
- Receiving and analyzing natural language queries about knowledge graphs
- Orchestrating the workflow between slave agents
- Making high-level decisions about query processing strategy
- Evaluating outputs from slave agents
- Synthesizing the final response to the user

### Slave Agents

Each agent is highly specialized and contributes to a specific part of the query processing pipeline:

1. **Query Refinement Agent**
   - Processes raw user queries and conversation history
   - Uses vector search to find similar examples
   - Transforms ambiguous or context-dependent queries into standalone, well-structured queries
   - Considers conversation context for query improvement

2. **Entity Recognition Agent**
   - Uses GLiNER model with ontology-specific entity types
   - Identifies knowledge graph-specific entities (classes, properties, instances, literals)
   - Extracts relevant terms from natural language
   - Determines query types and patterns

3. **Ontology Mapping Agent**
   - Uses embedding similarity and ontology structure
   - Maps extracted entities to specific ontology terms
   - Resolves ambiguities when multiple mappings exist
   - Handles synonyms and understands class hierarchies

4. **Tool Selection Agent**
   - Selects appropriate SPARQL templates and patterns
   - Uses vector similarity for template matching
   - Matches query intent to template patterns
   - Considers query complexity requirements

5. **Plan Formulation Agent**
   - Creates execution plans for queries
   - Generates step-by-step plans for execution
   - Handles complex queries requiring multiple SPARQL statements
   - Plans query optimization strategies

6. **Validation Agent**
   - Validates execution plans to prevent hallucinations
   - Checks logical consistency of plans
   - Ensures plan steps are appropriate for the query
   - Prevents invalid query constructions

7. **SPARQL Construction Agent**
   - Builds SPARQL queries based on templates and entities
   - Fills templates with entity values
   - Constructs syntactically correct SPARQL
   - Handles complex query components like FILTER, OPTIONAL, and UNION

8. **SPARQL Validation Agent**
   - Validates syntactic correctness of generated SPARQL
   - Checks semantic validity against the ontology
   - Ensures queries will execute correctly
   - Detects potential performance issues

9. **Query Execution Agent**
   - Executes SPARQL queries against configured endpoints
   - Handles authentication and rate limiting
   - Processes results and error handling
   - Manages query caching and optimization

10. **Response Generation Agent**
    - Transforms SPARQL results into natural language responses
    - Formats complex results into readable forms
    - Provides explanations of the query and results
    - Generates user-friendly responses

## Technology Stack

- **Agent Framework**: Microsoft AutoGen
- **Vector Database**: Qdrant for vector search of similar queries and patterns
- **Entity Resolution**: Elasticsearch for fuzzy search and handling misspellings
- **Triple Store**: RDF store for ontology access (can use GraphDB, Stardog, Apache Jena)
- **Embedding Models**:
  - BiEncoder for general semantic matching
  - CrossEncoder for precise reranking
- **Entity Recognition**: GLiNER (Generalist Language Interface for Named Entity Recognition)
- **Language Models**: GPT-3.5/4 for agents requiring reasoning and natural language processing

## Key Components

### Ontology Management

The system relies on access to ontology information:
- Class hierarchies
- Property domains and ranges
- Instance data
- Vocabulary and concept definitions

### SPARQL Templates

A collection of parameterized SPARQL query templates for common question types:
- Entity lookup ("What is X?")
- Relationship queries ("How are X and Y related?")
- Attribute queries ("What is the value of property P for entity E?")
- Filtering queries ("Which entities have property P greater than value V?")

### Query Patterns

Support for various SPARQL query forms:
- SELECT: Retrieving specific values
- ASK: Yes/no questions
- DESCRIBE: Getting all information about a resource
- CONSTRUCT: Creating new RDF graphs

## Setup and Installation

### Prerequisites

- Python 3.9 or higher
- Docker and Docker Compose
- At least 8GB of RAM recommended
- Basic knowledge of Docker commands

### 1. Docker Setup

First, set up the required services using Docker:

1. Create a `docker-compose.yml` file:
```yaml
version: '3.8'

services:
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage
    restart: unless-stopped

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.12.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9200:9200"
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
    restart: unless-stopped

  graphdb:
    image: ontotext/graphdb:10.6.0
    ports:
      - "7200:7200"
    environment:
      - GDB_HEAP_SIZE=4g
      - GDB_MIN_MEM=1g
      - GDB_MAX_MEM=4g
    volumes:
      - graphdb_data:/opt/graphdb/home
    restart: unless-stopped

volumes:
  qdrant_data:
  elasticsearch_data:
  graphdb_data:
```

2. Start the services:
```bash
docker-compose up -d
```

3. Verify the services are running:
- Qdrant: Visit `http://localhost:6333/dashboard`
- Elasticsearch: Visit `http://localhost:9200`
- GraphDB: Visit `http://localhost:7200`

### 2. Python Environment Setup

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration

Create a `.env` file with the following variables:
```bash
# OpenAI API
OPENAI_API_KEY=your_openai_api_key

# Database URLs
QDRANT_URL=http://localhost:6333
ELASTICSEARCH_URL=http://localhost:9200
GRAPHDB_URL=http://localhost:7200
GRAPHDB_REPOSITORY=your-repo-name
```

### 4. Initialize the System

1. Start the application:
```bash
python main.py
```

2. Access the Gradio interface at `http://localhost:7860`

### Troubleshooting

1. **Port Conflicts**: If you get port conflict errors, change the port mappings in the `docker-compose.yml` file.
2. **Memory Issues**: Adjust the memory settings in the `docker-compose.yml` if you encounter out-of-memory errors.
3. **Service Health**: Use `docker-compose ps` to check if all services are running properly.
4. **Cleanup**: To remove all containers and volumes:
```bash
docker-compose down -v
```

## Usage Examples

### Basic Queries

**User Query**: "What are all the subclasses of Person?"

**Generated SPARQL**:
```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ex: <http://example.org/ontology#>

SELECT ?subclass ?label
WHERE {
  ?subclass rdfs:subClassOf ex:Person .
  OPTIONAL { ?subclass rdfs:label ?label }
}
```

### Complex Queries

**User Query**: "Find all research papers published after 2020 with 'machine learning' in the title and authored by someone from Stanford University"

**Generated SPARQL**:
```sparql
PREFIX ex: <http://example.org/ontology#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?paper ?title ?date ?author ?authorName
WHERE {
  ?paper a ex:ResearchPaper ;
         ex:title ?title ;
         ex:publicationDate ?date ;
         ex:hasAuthor ?author .
  ?author ex:affiliation ?affiliation .
  ?affiliation rdfs:label ?affLabel .
  OPTIONAL { ?author ex:name ?authorName }
  
  FILTER (CONTAINS(LCASE(?title), "machine learning"))
  FILTER (?date >= "2020-01-01"^^xsd:date)
  FILTER (CONTAINS(LCASE(?affLabel), "stanford university"))
}
```

## Extending the System

### Adding New Ontologies

To support new knowledge domains:
1. Load new ontology files or configure access to ontology endpoints
2. Update entity recognition patterns for domain-specific terminology
3. Add domain-specific SPARQL templates

### Improving Query Understanding

To enhance natural language understanding:
1. Add more example mappings between natural language and SPARQL patterns
2. Fine-tune entity recognition for specific domains
3. Expand the template library with variations of common query patterns

## Architecture Advantages

- **Modularity**: Each agent handles a specific task, enabling focused development and testing
- **Extensibility**: Easy to add support for new ontologies or query patterns
- **Quality Control**: Validation ensures syntactically and semantically correct SPARQL
- **Explainability**: System can show the mapping from natural language to formal query elements
