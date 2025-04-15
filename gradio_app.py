import gradio as gr
import os

from main import (
    initialize_databases, initialize_models, initialize_tools,
    initialize_agents
)
from agents.query_execution import QueryExecutionAgent
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

# Set up GraphDB URL info as before.
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_REPO_ID = os.getenv("GRAPHDB_REPOSITORY")
GRAPHDB_ENDPOINT = os.path.join(GRAPHDB_URL, GRAPHDB_REPO_ID)

# --- Initialize all components like before ---
qdrant_client, elastic_client, ontology_store = initialize_databases()
bi_encoder, cross_encoder, entity_recognition_model = initialize_models()
template_tools, sparql_tools = initialize_tools()
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
# Initialize the query execution agent to run SPARQL against GraphDB.
query_execution_agent = QueryExecutionAgent(endpoint_url=GRAPHDB_ENDPOINT)

# --- Define processing functions for the Gradio UI ---

def process_gradio_query(user_query, conversation_history):
    """
    Process the natural language query and update conversation history.
    Returns: intermediate reasoning, final response, generated SPARQL query, updated history.
    """
    # Process the query using the master agent.
    # For detailed step output, ensure your master agent or agents provide a chain-of-thought.
    result = master_agent.process_query(user_query, conversation_history)
    
    # Here we assume that your processing returns a dictionary containing:
    # - "chain_of_thought": a string representing intermediate reasoning
    # - "response": the final answer to the query
    # - "sparql": the final SPARQL query (or list of queries)
    # Adjust the keys based on your actual implementation.
    intermediate_reasoning = result.get("chain_of_thought", "Intermediate reasoning not provided.")
    final_response = result.get("response", "No final response.")
    generated_sparql = result.get("sparql", "No SPARQL query generated.")

    # Update conversation history with the interaction.
    conversation_history.append({"role": "user", "content": user_query})
    conversation_history.append({"role": "assistant", "content": final_response})
    
    return intermediate_reasoning, final_response, generated_sparql, conversation_history

def execute_sparql_query(sparql_query):
    """
    Execute a SPARQL query on GraphDB using the preconfigured QueryExecutionAgent.
    """
    try:
        # Execute the query. The agent can return raw results or formatted data.
        execution_result = query_execution_agent.execute(sparql_query)
        # For display, convert the result to a string. You might want to format it better.
        return str(execution_result)
    except Exception as e:
        logger.error(f"Error executing SPARQL: {e}")
        return f"Error executing SPARQL: {e}"

# --- Construct the Gradio Blocks interface ---
with gr.Blocks() as demo:
    gr.Markdown("# Natural Language to SPARQL Interactive UI")
    
    with gr.Tab("Query Chat"):
        with gr.Row():
            user_input = gr.Textbox(label="Enter your question", placeholder="Type your query here...", lines=1)
            submit_button = gr.Button("Submit")
        with gr.Row():
            reasoning_output = gr.Textbox(label="Intermediate Reasoning", lines=5)
        with gr.Row():
            final_answer_output = gr.Textbox(label="Final Answer", lines=3)
        with gr.Row():
            sparql_output = gr.Textbox(label="Generated SPARQL Query", lines=5)
        
        # Use a Gradio State to maintain the conversation history.
        conversation_state = gr.State([])

        submit_button.click(
            process_gradio_query,
            inputs=[user_input, conversation_state],
            outputs=[reasoning_output, final_answer_output, sparql_output, conversation_state]
        )
    
    with gr.Tab("Execute SPARQL"):
        gr.Markdown("Enter a SPARQL query (this can be the generated query or your own) to execute on GraphDB.")
        with gr.Row():
            sparql_input = gr.Textbox(label="SPARQL Query", placeholder="Type your SPARQL query here...", lines=5)
            execute_button = gr.Button("Execute Query")
        sparql_result = gr.Textbox(label="Query Result", lines=5)
        execute_button.click(
            execute_sparql_query,
            inputs=[sparql_input],
            outputs=[sparql_result]
        )

# Launch the Gradio demo.
if __name__ == "__main__":
    demo.launch()
