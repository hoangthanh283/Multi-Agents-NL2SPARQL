import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import gradio as gr
import pandas as pd

from main import (initialize_agents, initialize_databases, initialize_models,
                  initialize_tools)
from utils.logging_utils import setup_logging


# Set up custom logger for capturing intermediate steps
class StreamToQueue:
    def __init__(self):
        self.logs = []
        self.reasoning_steps = []
    
    def write(self, text):
        self.logs.append(text)
        # Identify reasoning steps by patterns
        if "Agent:" in text or "Processing:" in text or "Generated:" in text:
            self.reasoning_steps.append(text)
    
    def flush(self):
        pass
    
    def get_logs(self):
        return "\n".join(self.logs)
    
    def get_reasoning(self):
        return "\n".join(self.reasoning_steps)
    
    def clear(self):
        self.logs = []
        self.reasoning_steps = []

# Setup logging
logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

# Initialize the system components
qdrant_client, elastic_client, ontology_store = initialize_databases()
bi_encoder, cross_encoder, entity_recognition_model = initialize_models()
template_tools, sparql_tools = initialize_tools()

# Initialize the master agent
master_agent = initialize_agents(
    qdrant_client, 
    elastic_client,
    ontology_store, 
    bi_encoder, 
    cross_encoder, 
    entity_recognition_model,
    template_tools
)

# Track conversation history
conversation_history = []

# Create a stream capture for logging
log_capture = StreamToQueue()

def process_query(query: str) -> Tuple[str, str, str, List[Dict[str, str]]]:
    """
    Process a query and return response, reasoning, SPARQL, and conversation history.
    
    Args:
        query: User query string
        
    Returns:
        Tuple of (response text, reasoning steps, SPARQL query, updated conversation history)
    """
    global conversation_history
    
    # Clear the log capture
    log_capture.clear()
    
    try:
        # Process the query
        result = master_agent.process_query(query, conversation_history)
        
        # Extract the response and SPARQL query from the result dictionary
        response = result["response"] if "response" in result else "Sorry, I couldn't process that query."
        sparql_query = result["sparql"] if "sparql" in result else "No SPARQL query generated."
        
        # Extract reasoning steps from logs
        reasoning = log_capture.get_reasoning()
        if not reasoning:
            reasoning = "No intermediate reasoning steps captured."
        
        # Update conversation history
        conversation_history.append({
            "role": "user",
            "content": query
        })
        conversation_history.append({
            "role": "assistant",
            "content": response
        })
        
        # Format for display in UI
        chat_history = [
            [message["content"], response] 
            for i, message in enumerate(conversation_history) 
            if message["role"] == "user"
        ]
        
        # Extract multiple SPARQL queries if present
        sparql_queries = extract_sparql_queries(sparql_query)
        
        return response, reasoning, sparql_queries, chat_history
    
    except Exception as e:
        error_message = f"Error processing query: {str(e)}"
        logger.error(error_message)
        return error_message, "An error occurred during processing.", "No SPARQL query generated.", conversation_history

def extract_sparql_queries(text: str) -> List[Dict[str, str]]:
    """
    Extract SPARQL queries from text and format them for display.
    
    Args:
        text: Text potentially containing SPARQL queries
        
    Returns:
        List of dictionaries with query info
    """
    # Simple pattern for extracting SPARQL queries
    pattern = r"(?:```(?:sparql)?\s*)?((?:PREFIX|SELECT|ASK|CONSTRUCT|DESCRIBE)[\s\S]+?(?:LIMIT|\}))(?:\s*```|$)"
    matches = re.finditer(pattern, text, re.IGNORECASE)
    
    queries = []
    for i, match in enumerate(matches, 1):
        query_text = match.group(1).strip()
        queries.append({
            "id": f"query_{i}",
            "name": f"Query {i}",
            "sparql": query_text,
            "description": f"Generated SPARQL query {i}"
        })
    
    # If no queries found but text looks like a SPARQL query
    if not queries and any(keyword in text.upper() for keyword in ["SELECT", "ASK", "CONSTRUCT", "DESCRIBE"]):
        queries.append({
            "id": "query_1",
            "name": "Query 1",
            "sparql": text.strip(),
            "description": "Generated SPARQL query"
        })
    
    return queries

def execute_sparql_query(query: str, endpoint_url: Optional[str] = None) -> Tuple[str, pd.DataFrame]:
    """
    Execute a SPARQL query against an endpoint.
    
    Args:
        query: SPARQL query string
        endpoint_url: Optional URL for SPARQL endpoint
        
    Returns:
        Tuple of (result message, results as DataFrame)
    """
    try:
        # Use the query_execution_agent to execute the query
        query_execution_agent = master_agent.slave_agents["query_execution"]
        result = query_execution_agent.execute_query(
            sparql_query=query,
            endpoint_url=endpoint_url
        )
        
        # Check if execution was successful
        if not result.get("success", False):
            error_message = result.get("error", "Unknown error executing query")
            return f"Error: {error_message}", pd.DataFrame()
        
        # Process the results
        results_data = result.get("results", {})
        
        # Convert results to DataFrame
        if isinstance(results_data, dict) and "bindings" in results_data:
            rows = []
            variables = results_data.get("variables", [])
            
            for binding in results_data["bindings"]:
                row = {}
                for var in variables:
                    if var in binding:
                        value = binding[var].get("value", "")
                        row[var] = value
                    else:
                        row[var] = ""
                rows.append(row)
            
            df = pd.DataFrame(rows)
            return f"Query returned {len(rows)} results", df
        
        # Handle other result types
        return "Query executed successfully", pd.DataFrame([{"result": str(results_data)}])
    
    except Exception as e:
        error_message = f"Error executing query: {str(e)}"
        logger.error(error_message)
        return error_message, pd.DataFrame()

def copy_to_clipboard(text: str) -> str:
    """Function to copy text to clipboard (for the UI)"""
    return text

def clear_conversation() -> Tuple[List, str, str, str, List]:
    """Clear the conversation history"""
    global conversation_history
    conversation_history = []
    return [], "", "", "", []

def create_ui():
    """Create the Gradio UI"""
    with gr.Blocks(title="Natural Language to SPARQL Converter") as app:
        gr.Markdown("# Natural Language to SPARQL Converter")
        gr.Markdown("Ask questions about the knowledge graph in natural language and get SPARQL queries.")
        
        with gr.Row():
            with gr.Column(scale=2):
                chatbot = gr.Chatbot(
                    label="Conversation",
                    height=500
                )
                
                with gr.Row():
                    query_input = gr.Textbox(
                        label="Your Question",
                        placeholder="e.g., What are the properties of the Person class?",
                        lines=2
                    )
                    submit_btn = gr.Button("Submit", variant="primary")
                
                clear_btn = gr.Button("Clear Conversation")
                
            with gr.Column(scale=1):
                with gr.Tab("Generated SPARQL"):
                    sparql_output = gr.Code(
                        label="SPARQL Query", 
                        language="sparql",
                        lines=10
                    )
                    copy_btn = gr.Button("Copy to Clipboard")
                    graphdb_link = gr.HTML("""
                    <div style="margin-top: 10px; text-align: center;">
                        <a href="http://localhost:7200/sparql" target="_blank" style="text-decoration: none;">
                            <button style="background-color: #4CAF50; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer;">
                                Open in GraphDB
                            </button>
                        </a>
                    </div>
                    """)
                
                with gr.Tab("Reasoning Steps"):
                    reasoning_output = gr.Textbox(
                        label="System Reasoning", 
                        lines=20, 
                        max_lines=30
                    )
                
                with gr.Tab("Query Results"):
                    endpoint_input = gr.Textbox(
                        label="SPARQL Endpoint URL (optional)",
                        placeholder="e.g., http://localhost:7200/repositories/myrepo",
                        value=""
                    )
                    execute_btn = gr.Button("Execute Query")
                    result_message = gr.Textbox(label="Result Message")
                    result_table = gr.DataFrame(label="Results")
        
        # Set up event handlers
        submit_btn.click(
            process_query,
            inputs=[query_input],
            outputs=[chatbot, reasoning_output, sparql_output, chatbot]
        )
        
        clear_btn.click(
            clear_conversation,
            inputs=[],
            outputs=[chatbot, query_input, reasoning_output, sparql_output, chatbot]
        )
        
        copy_btn.click(
            copy_to_clipboard,
            inputs=[sparql_output],
            outputs=[sparql_output]
        )
        
        execute_btn.click(
            execute_sparql_query,
            inputs=[sparql_output, endpoint_input],
            outputs=[result_message, result_table]
        )
    return app


if __name__ == "__main__":
    # Set the GraphDB endpoint URL from environment or use default
    graphdb_endpoint = os.getenv("GRAPHDB_ENDPOINT", "http://localhost:7200/repositories/academic")
    
    # Create and launch the app
    app = create_ui()
    app.launch(share=False)
