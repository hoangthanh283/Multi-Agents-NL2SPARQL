import os
import warnings
warnings.filterwarnings("ignore")
os.environ["AUTOGEN_USE_DOCKER"] = "False"

from dotenv import load_dotenv
load_dotenv()
import time

from agents.master_agent_2 import MasterAgent
from agents.plan_formulation_2 import PlanFormulationAgent
from agents.validation_2 import ValidationAgent
from agents.response_generation_2 import ResponseGenerationAgent
from agents.entity_recognition import EntityRecognitionAgent
from agents.query_refinement import QueryRefinementAgent
from models.embeddings import BiEncoderModel, CrossEncoderModel
from models.entity_recognition import GLiNERModel
from utils.constants import QDRANT_COLLECTIONS
from database.qdrant_client import QdrantClient
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema
from langchain.output_parsers import StructuredOutputParser
from database.qdrant_client import QdrantClient
from typing import List, Dict, Any, Optional
import json
from utils.constants import QDRANT_COLLECTIONS
from utils.logging_utils import setup_logging
from tqdm import tqdm
from qdrant_client.models import Distance, VectorParams, PointStruct



logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

def initialize_databases():
    """Initialize vector database, elastic search and ontology store."""
    logger.info("Initializing databases...")
    # Initialize Qdrant client.
    qdrant_client = QdrantClient(url=os.getenv("QDRANT_URL"))
    for collection in QDRANT_COLLECTIONS:
        if not qdrant_client.collection_exists(collection):
            logger.info(f"Creating Qdrant collection: {collection}")
            qdrant_client.create_collection(collection)

            if collection == "ontology_embedding":
                assert os.path.exists("assets/ontologies/CHeVIE_comment.owl")
                with open("assets/ontologies/CHeVIE_comment.owl", "r") as f:
                    data = f.read()
                code_parts = data.split("\n\n\n")
                points = []

                for idx, code_part in tqdm(enumerate(code_parts)):
                    embedding = qdrant_client.default_model.encode(code_part)
                    point = {
                        "id": idx,
                        "vector": embedding.tolist(),
                        "payload": {"code": code_part}
                    }
                    points.append(point)

                qdrant_client.upsert_points(
                    collection,
                    points
                )
    
    return qdrant_client

def initialize_models():
    """Initialize embedding and entity recognition models."""
    logger.info("Initializing models...")
    bi_encoder = BiEncoderModel(
        model_name_or_path="sentence-transformers/all-MiniLM-L6-v2"
    )
    
    # Initialize Cross-Encoder for reranking
    cross_encoder = CrossEncoderModel(
        model_name_or_path="cross-encoder/ms-marco-MiniLM-L-6-v2"
    )
    
    # Initialize entity recognition model
    entity_recognition_model = GLiNERModel(
        model_name_or_path="urchade/gliner_medium-v2.1"
    )
    return bi_encoder, cross_encoder, entity_recognition_model

def create_master_agent(qdrant_client, bi_encoder, entity_recognition_model, cross_encoder):
    master_agent = MasterAgent()
    query_refinement_agent = QueryRefinementAgent(
        qdrant_client=qdrant_client,
        embedding_model=bi_encoder
    )
    
    entity_recognition_agent = EntityRecognitionAgent(
        entity_recognition_model=entity_recognition_model, 
        ontology_store=None
    )
    plan_formulation_agent = PlanFormulationAgent()
    validation_agent = ValidationAgent()
    response_generation_agent = ResponseGenerationAgent()
    master_agent.register_slave_agent("query_refinement", query_refinement_agent)
    master_agent.register_slave_agent("entity_recognition", entity_recognition_agent)
    master_agent.register_slave_agent("plan_formulation", plan_formulation_agent)
    master_agent.register_slave_agent("validation", validation_agent)
    master_agent.register_slave_agent("response_generation", response_generation_agent)
    return master_agent

def process_query(master_agent, query, conversation_history=None):
    """Process a natural language query and return SPARQL and results."""
    if conversation_history is None:
        conversation_history = []

    result = master_agent.process_query(query, conversation_history)
    logger.info(f"Processed query: {query}")
    logger.info("Generated SPARQL: {}".format(result["response"][-1].get("query", "No SPARQL generated")))
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
            ans = result.get("response", "Sorry, I couldn't process that query.")
            if isinstance(ans, list):
                ans = ans[-1].get("query", "No SPARQL generated")
            logger.info("\nSPARQL Query:" + ans)
            
            # Update conversation history.
            conversation_history.append({
                "role": "user",
                "content": user_query
            })
            conversation_history.append({
                "role": "assistant",
                "content": ans
            })
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            logger.info(f"Sorry, there was an error processing your query: {str(e)}")
    logger.info("Interactive session ended.")

    return result

def main():
    """Main entry point for the NL to SPARQL conversion system."""
    logger.info("Starting Natural Language to SPARQL conversion system...")
    
    # Initialize components
    qdrant_client = initialize_databases()
    bi_encoder, cross_encoder, entity_recognition_model = initialize_models()
    
    # Initialize master agent with all slave agents
    master_agent = create_master_agent(
        qdrant_client, bi_encoder, entity_recognition_model, cross_encoder
    )  
    interactive_session(master_agent)
    logger.info("NL to SPARQL conversion system terminated.")


if __name__ == "__main__":
    main()