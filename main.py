import os

from agents.entity_recognition import EntityRecognitionAgent
from agents.master_agent import MasterAgent
from agents.ontology_mapping import OntologyMappingAgent
from agents.plan_formulation import PlanFormulationAgent
from agents.query_execution import QueryExecutionAgent
from agents.query_refinement import QueryRefinementAgent
from agents.response_generation import ResponseGenerationAgent
from agents.sparql_construction import SPARQLConstructionAgent
from agents.sparql_validation import SPARQLValidationAgent
from agents.tool_execution import ToolExecutionAgent
from agents.tool_selection import ToolSelectionAgent
from agents.validation import ValidationAgent
from database.elastic_client import ElasticClient
from database.ontology_store import OntologyStore
from database.qdrant_client import QdrantClient
from models.embeddings import BiEncoderModel, CrossEncoderModel
from models.entity_recognition import GLiNERModel
from tools.sparql_tools import SPARQLTools
from tools.template_tools import TemplateTools
from utils.constants import QDRANT_COLLECTIONS
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_REPO_ID = os.getenv("GRAPHDB_REPOSITORY")
GRAPHDB_ENDPOINT = os.path.join(GRAPHDB_URL, GRAPHDB_REPO_ID)


def initialize_databases():
    """Initialize vector database, elastic search and ontology store."""
    logger.info("Initializing databases...")
    # Initialize Qdrant client.
    qdrant_client = QdrantClient(url=os.getenv("QDRANT_URL"))
    for collection in QDRANT_COLLECTIONS:
        if not qdrant_client.collection_exists(collection):
            logger.info(f"Creating Qdrant collection: {collection}")
            qdrant_client.create_collection(collection)

    # Initialize Elasticsearch client.
    elastic_client = ElasticClient(url=os.getenv("ELASTICSEARCH_URL"))
    elastic_client.initialize_indices()

    # Initialize ontology store with GraphDB support.
    ontology_store = OntologyStore(endpoint_url=GRAPHDB_ENDPOINT)

    # Load ontology data.
    load_success = ontology_store.load_ontology()
    if not load_success:
        logger.error("Failed to load ontology from GraphDB, falling back to local file")
        ontology_path = os.getenv("ONTOLOGY_PATH", "data/ontologies/academic_ontology.ttl")
        ontology_store = OntologyStore(local_path=ontology_path)
        ontology_store.load_ontology()
    return qdrant_client, elastic_client, ontology_store


def initialize_models():
    """Initialize embedding and entity recognition models."""
    logger.info("Initializing models...")
    
    # Initialize Bi-Encoder model for query embedding
    bi_encoder = BiEncoderModel(
        model_name_or_path="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    # Initialize Cross-Encoder for reranking
    cross_encoder = CrossEncoderModel(
        model_name_or_path="cross-encoder/ms-marco-MiniLM-L-6-v2"
    )
    
    # Initialize entity recognition model
    entity_recognition_model = GLiNERModel(
        model_name_or_path="urchade/gliner_small-v1"
    )
    return bi_encoder, cross_encoder, entity_recognition_model


def initialize_tools():
    """Initialize tools for SPARQL templates and utilities."""
    logger.info("Initializing tools...")
    
    # Initialize template tools
    template_tools = TemplateTools(templates_dir="templates/sparql")
    
    # Initialize SPARQL tools
    sparql_tools = SPARQLTools()
    return template_tools, sparql_tools


def initialize_agents(qdrant_client, elastic_client, ontology_store, bi_encoder, cross_encoder,
                      entity_recognition_model, template_tools, sparql_tools):
    """Initialize the master agent and all slave agents."""
    logger.info("Initializing agents...")

    # Initialize master agent.
    master_agent = MasterAgent()
    
    # Initialize slave agents
    query_refinement_agent = QueryRefinementAgent(
        qdrant_client=qdrant_client,
        embedding_model=bi_encoder
    )
    
    entity_recognition_agent = EntityRecognitionAgent(
        entity_recognition_model=entity_recognition_model, 
        ontology_store=ontology_store
    )
    
    # Initialize ontology mapping agent with correct parameters.
    ontology_mapping_agent = OntologyMappingAgent(
        ontology_path=ontology_store.local_path,
        ontology_endpoint=ontology_store.endpoint_url,
        embedding_model_name="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    tool_selection_agent = ToolSelectionAgent(
        qdrant_client=qdrant_client,
        embedding_model=bi_encoder,
        reranking_model=cross_encoder,
        template_tools=template_tools
    )
    
    plan_formulation_agent = PlanFormulationAgent(
        template_tools=template_tools
    )
    
    validation_agent = ValidationAgent()
    sparql_construction_agent = SPARQLConstructionAgent(templates_dir="templates/sparql") 
    sparql_validation_agent = SPARQLValidationAgent()
    query_execution_agent = QueryExecutionAgent(endpoint_url=GRAPHDB_ENDPOINT)
    tool_execution_agent = ToolExecutionAgent(endpoint_url=GRAPHDB_ENDPOINT)
    response_generation_agent = ResponseGenerationAgent()

    # Register slave agents with master agent.
    master_agent.register_slave_agent("query_refinement", query_refinement_agent)
    master_agent.register_slave_agent("entity_recognition", entity_recognition_agent)
    master_agent.register_slave_agent("ontology_mapping", ontology_mapping_agent)
    master_agent.register_slave_agent("tool_selection", tool_selection_agent)
    master_agent.register_slave_agent("plan_formulation", plan_formulation_agent)
    master_agent.register_slave_agent("validation", validation_agent)
    master_agent.register_slave_agent("sparql_construction", sparql_construction_agent)
    master_agent.register_slave_agent("sparql_validation", sparql_validation_agent)
    master_agent.register_slave_agent("query_execution", query_execution_agent)
    master_agent.register_slave_agent("tool_execution", tool_execution_agent)
    master_agent.register_slave_agent("response_generation", response_generation_agent)
    master_agent.register_slave_agent("sparql_tools", sparql_tools)
    master_agent.register_slave_agent("elastic_client", elastic_client)
    return master_agent


def process_query(master_agent, query, conversation_history=None):
    """Process a natural language query and return SPARQL and results."""
    if conversation_history is None:
        conversation_history = []

    result = master_agent.process_query(query, conversation_history)
    logger.info(f"Processed query: {query}")
    logger.info(f"Generated SPARQL: {result.get('sparql', 'No SPARQL generated')}")
    return result


def interactive_session(master_agent):
    """Start an interactive session for processing queries."""
    logger.info("Starting interactive session...")
    
    # Maintain conversation history
    conversation_history = []
    logger.info("\nWelcome to the Natural Language to SPARQL Converter!")
    logger.info("Enter your questions about the knowledge graph, or type 'exit' to quit.\n")
    while True:
        # Get user input
        user_query = input("Your question: ")
        
        # Check for exit command
        if user_query.lower() in ['exit', 'quit', 'bye']:
            logger.info("Goodbye!")
            break
        
        try:
            result = process_query(master_agent, user_query, conversation_history)
            logger.info("\n" + result.get("response", "Sorry, I couldn't process that query."))
            
            # Show the SPARQL if requested.
            if "show sparql" in user_query.lower() or "show query" in user_query.lower():
                sparql = result.get("sparql", "No SPARQL query generated.")
                logger.info("\nSPARQL Query:")
                logger.info(sparql)
            
            # Update conversation history
            conversation_history.append({
                "role": "user",
                "content": user_query
            })
            conversation_history.append({
                "role": "assistant",
                "content": result.get("response", "")
            })
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            logger.info(f"Sorry, there was an error processing your query: {str(e)}")
    logger.info("Interactive session ended.")


def main():
    """Main entry point for the NL to SPARQL conversion system."""
    logger.info("Starting Natural Language to SPARQL conversion system...")
    
    # Initialize components
    qdrant_client, elastic_client, ontology_store = initialize_databases()
    bi_encoder, cross_encoder, entity_recognition_model = initialize_models()
    template_tools, sparql_tools = initialize_tools()
    
    # Initialize master agent with all slave agents
    master_agent = initialize_agents(
        qdrant_client, 
        elastic_client,
        ontology_store, 
        bi_encoder, 
        cross_encoder, 
        entity_recognition_model,
        template_tools,
        sparql_tools
    )  
    interactive_session(master_agent)
    logger.info("NL to SPARQL conversion system terminated.")


if __name__ == "__main__":
    main()
