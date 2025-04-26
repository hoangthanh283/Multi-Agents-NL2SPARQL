# Natural Language to SPARQL Converter

A multi-agent system for converting natural language queries into SPARQL queries for knowledge graph exploration, implemented using a Master-Slave architecture with AutoGen.

- Refactored to a master-slave architecture with a global master (API), three domain masters (NLP, Query, Response), and specialized slave agents.
- Introduced the `adapters/` layer to wrap existing agents for use in the master-slave workflow.
- Modularized domain logic in `master/` (base, nlp_master, query_master, response_master).
- Added Prometheus metrics and Grafana dashboards for all major components.
- Provided full Kubernetes manifests for scalable, production-ready deployment.

---

## Architecture Overview

This system now uses a hierarchical master-slave architecture:
- **Global Master (API)**: Orchestrates the full NL2SPARQL workflow, manages workflow state in Redis, and delegates to domain masters.
- **Domain Masters (NLP, Query, Response)**: Each coordinates a domain-specific workflow, dispatches tasks to slave pools, and aggregates results.
- **Slave Agents**: Specialized agents (wrapped via `adapters/agent_adapter.py`) that perform atomic tasks (e.g., query refinement, entity recognition, SPARQL construction, etc.).

**Communication:**
- Redis pub/sub is used for workflow and task messaging between masters and slaves.
- Celery is used for distributed task execution.
- Prometheus and Grafana provide real-time monitoring and dashboards.

### System Architecture Diagram

```
                        +------------------+
                        |  Load Balancer   |
                        +--------+---------+
                                 |
                +----------------+----------------+
                |                                 |
        +-------v--------+                +-------v--------+
        | Global Master  |                | Global Master  |
        +-------+--------+                +-------+--------+
                |                                 |
        +-------v--------+                +-------v--------+
        | Domain Master 1|                | Domain Master 2|
        +-------+--------+                +-------+--------+
            |       |                         |        |
   +--------v+   +--v--------+        +-------v+   +----v-------+
   |Slave    |   |Slave      |        |Slave   |   |Slave       |
   |Pool 1   |   |Pool 2     |        |Pool 3  |   |Pool 4      |
   +---------+   +-----------+        +--------+   +------------+
```

---

## Component Overview
- `adapters/`: Contains `agent_adapter.py`, which wraps existing agents for use in the master-slave system.
- `master/`: Contains the base class and domain master implementations (`base.py`, `nlp_master.py`, `query_master.py`, `response_master.py`).
- `k8s/`: Kubernetes manifests for all services, including API, domain masters, workers, Redis, GraphDB, Prometheus, and Grafana.
- `utils/monitoring.py`: Prometheus metrics and monitoring logic for all major components.

---

## Monitoring & Observability
- Prometheus metrics are exposed for:
  - Workflow and task counts, processing times, and error rates (per domain and agent)
  - System resource usage (CPU, memory, etc.)
- Grafana dashboards are available for real-time system health and performance.
- See the "Kubernetes Deployment & Testing" section for instructions on accessing dashboards.

---

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
    image: ontotext/graphdb:10.6.4
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

## Kubernetes Deployment & Testing

### Prerequisites
- Ubuntu 20.04 or later
- [Docker](https://docs.docker.com/engine/install/ubuntu/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- (Optional) [Helm](https://helm.sh/docs/intro/install/) for advanced management

### 1. Start Minikube
```sh
minikube start --driver=docker
```

### 2. Build Docker Images for Minikube
```sh
eval $(minikube docker-env)
docker build -t nl2sparql-api:latest -f Dockerfile .
docker build -t nl2sparql-worker:latest -f Dockerfile.worker .
```

### 3. Deploy Dependencies
```sh
kubectl apply -f k8s/redis-statefulset.yml
kubectl apply -f k8s/redis-service.yml
kubectl apply -f k8s/graphdb-statefulset.yml
kubectl apply -f k8s/graphdb-service.yml
# (Optional) If using Kafka, Dask, Ray:
kubectl apply -f k8s/kafka-deployment.yml
kubectl apply -f k8s/dask-deployment.yml
kubectl apply -f k8s/ray-deployment.yml
```

### 4. Deploy Core Services
```sh
kubectl apply -f k8s/api-deployment.yml
kubectl apply -f k8s/api-service.yml
kubectl apply -f k8s/domain-masters-deployment.yml
kubectl apply -f k8s/worker-deployment.yml
```

### 5. Deploy Monitoring
```sh
kubectl apply -f k8s/prometheus-config.yml
kubectl apply -f k8s/grafana-config.yml
```

### 6. Expose and Test the API
```sh
kubectl port-forward svc/nl2sparql-api 8000:8000
# In another terminal:
curl -X POST "http://localhost:8000/api/nl2sparql" -H "Content-Type: application/json" -d '{"query": "What are the symptoms of COVID-19?", "context": []}'
```

### 7. Access Monitoring Dashboards
```sh
kubectl port-forward svc/prometheus 9090:9090
kubectl port-forward svc/grafana 3000:3000
# Then visit http://localhost:9090 (Prometheus) or http://localhost:3000 (Grafana)
```

### 8. Cleanup
```sh
kubectl delete -f k8s/
minikube stop
```

---

This section provides a full local deployment and testing workflow for Kubernetes. For troubleshooting, check pod logs with `kubectl logs <pod-name>` and verify service status with `kubectl get pods` and `kubectl get svc`.
