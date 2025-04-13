import logging
import os
from typing import Any, Dict, List

import autogen

from agents.entity_recognition import EntityRecognitionAgent
# Import agent components
from agents.master_agent import MasterAgent
from agents.ontology_mapping import OntologyMappingAgent
from agents.query_execution import QueryExecutionAgent
from agents.query_refinement import QueryRefinementAgent
from agents.response_generation import ResponseGenerationAgent
from agents.sparql_construction import SPARQLConstructionAgent
from agents.sparql_validation import SPARQLValidationAgent
from database.ontology_store import OntologyStore
# Import database components
from database.qdrant_client import QdrantClient
# Import model components
from models.embeddings import BiEncoderModel, CrossEncoderModel
from models.entity_recognition import GLiNERModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_databases():
    """Initialize vector database and ontology store."""
    logger.info("Initializing databases...")
    
    # Initialize Qdrant client
    qdrant_client = QdrantClient(
        url=os.getenv("QDRANT_URL"),
        api_key=os.getenv("QDRANT_API_KEY")
    )
    
    # Create collections if they don't exist
    collections = ["query_patterns", "sparql_examples", "conversation_history"]
    for collection in collections:
        if not qdrant_client.collection_exists(collection):
            logger.info(f"Creating Qdrant collection: {collection}")
            qdrant_client.create_collection(collection)
    
    # Initialize ontology store
    ontology_path = os.getenv("ONTOLOGY_PATH")
    ontology_endpoint = os.getenv("SPARQL_ENDPOINT")
    
    ontology_store = OntologyStore(
        local_path=ontology_path,
        endpoint_url=ontology_endpoint
    )
    
    # Load ontology data
    ontology_store.load_ontology()
    
    return qdrant_client, ontology_store

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
        model_name_or_path="microsoft/gliner"
    )
    
    return bi_encoder, cross_encoder, entity_recognition_model

def initialize_agents(qdrant_client, ontology_store, bi_encoder, cross_encoder, entity_recognition_model):
    """Initialize the master agent and all slave agents."""
    logger.info("Initializing agents...")
    
    # Initialize SPARQL endpoint for query execution
    sparql_endpoint = os.getenv("SPARQL_ENDPOINT")
    sparql_auth_token = os.getenv("SPARQL_AUTH_TOKEN")
    
    # Initialize master agent
    master_agent = MasterAgent()
    
    # Initialize slave agents
    query_refinement_agent = QueryRefinementAgent(qdrant_client)
    
    entity_recognition_agent = EntityRecognitionAgent(
        entity_recognition_model, 
        ontology_store
    )
    
    ontology_mapping_agent = OntologyMappingAgent(
        ontology_store=ontology_store,
        embedding_model_name="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    sparql_construction_agent = SPARQLConstructionAgent(
        templates_dir="templates/sparql"
    )
    
    sparql_validation_agent = SPARQLValidationAgent()
    
    query_execution_agent = QueryExecutionAgent(
        endpoint_url=sparql_endpoint,
        auth_token=sparql_auth_token
    )
    
    response_generation_agent = ResponseGenerationAgent()
    
    # Register slave agents with master agent
    master_agent.register_slave_agent("query_refinement", query_refinement_agent)
    master_agent.register_slave_agent("entity_recognition", entity_recognition_agent)
    master_agent.register_slave_agent("ontology_mapping", ontology_mapping_agent)
    master_agent.register_slave_agent("sparql_construction", sparql_construction_agent)
    master_agent.register_slave_agent("sparql_validation", sparql_validation_agent)
    master_agent.register_slave_agent("query_execution", query_execution_agent)
    master_agent.register_slave_agent("response_generation", response_generation_agent)
    
    return master_agent

def process_query(master_agent, query, conversation_history=None):
    """Process a natural language query and return SPARQL and results."""
    if conversation_history is None:
        conversation_history = []
    
    # Process the query
    result = master_agent.process_query(query, conversation_history)
    
    # Log the processed steps
    logger.info(f"Processed query: {query}")
    logger.info(f"Generated SPARQL: {result.get('sparql', 'No SPARQL generated')}")
    
    return result

def interactive_session(master_agent):
    """Start an interactive session for processing queries."""
    logger.info("Starting interactive session...")
    
    # Maintain conversation history
    conversation_history = []
    
    print("\nWelcome to the Natural Language to SPARQL Converter!")
    print("Enter your questions about the knowledge graph, or type 'exit' to quit.\n")
    
    while True:
        # Get user input
        user_query = input("Your question: ")
        
        # Check for exit command
        if user_query.lower() in ['exit', 'quit', 'bye']:
            print("Goodbye!")
            break
        
        try:
            # Process the query
            result = process_query(master_agent, user_query, conversation_history)
            
            # Display the result to the user
            print("\n" + result.get("response", "Sorry, I couldn't process that query."))
            
            # Show the SPARQL if requested
            if "show sparql" in user_query.lower() or "show query" in user_query.lower():
                sparql = result.get("sparql", "No SPARQL query generated.")
                print("\nSPARQL Query:")
                print(sparql)
            
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
            print(f"Sorry, there was an error processing your query: {str(e)}")
    
    logger.info("Interactive session ended.")

def main():
    """Main entry point for the NL to SPARQL conversion system."""
    logger.info("Starting Natural Language to SPARQL conversion system...")
    
    # Initialize components
    qdrant_client, ontology_store = initialize_databases()
    bi_encoder, cross_encoder, entity_recognition_model = initialize_models()
    
    # Initialize master agent with all slave agents
    master_agent = initialize_agents(
        qdrant_client, 
        ontology_store, 
        bi_encoder, 
        cross_encoder, 
        entity_recognition_model
    )
    
    # Start interactive session
    interactive_session(master_agent)
    logger.info("NL to SPARQL conversion system terminated.")


if __name__ == "__main__":
    main()
