import json
import os
import time
from typing import Any, Dict, List, Optional

import gradio as gr
import requests

from utils.logging_utils import setup_logging

# Configure logging
logger = setup_logging(app_name="nl-to-sparql-ui", enable_colors=True)

# API URLs
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
NL2SPARQL_ENDPOINT = f"{API_BASE_URL}/api/nl2sparql"
SPARQL_EXEC_ENDPOINT = f"{API_BASE_URL}/api/sparql"
API_HEALTH_ENDPOINT = f"{API_BASE_URL}/health"

# Constants
POLLING_INTERVAL = 1.0  # seconds
MAX_POLLING_TIME = 60.0  # seconds
GRAPHDB_URL = os.getenv("GRAPHDB_URL", "http://localhost:7200")
GRAPHDB_REPO_ID = os.getenv("GRAPHDB_REPOSITORY", "CHeVIE")
GRAPHDB_ENDPOINT = f"{GRAPHDB_URL}/repositories/{GRAPHDB_REPO_ID}"


def process_gradio_query(user_query, conversation_history, max_wait=MAX_POLLING_TIME):
    """
    Process the natural language query using the NL2SPARQL API.
    
    Args:
        user_query: The user's query string
        conversation_history: The conversation history
        max_wait: Maximum time to wait for results (seconds)
        
    Returns:
        intermediate_reasoning, final_answer, generated_sparql, updated_history
    """
    try:
        # Add context from conversation history if available
        context = []
        if conversation_history:
            # Extract last 3 exchanges to use as context
            for exchange in conversation_history[-6:]:
                if exchange.get("content"):
                    context.append(exchange["content"])
        
        # First, submit the query to the API
        logger.info(f"Submitting query to API: {user_query}")
        response = requests.post(
            NL2SPARQL_ENDPOINT,
            json={"query": user_query, "context": context}
        )
        response.raise_for_status()
        request_id = response.json().get("request_id")
        
        if not request_id:
            raise Exception("No request ID returned from the API")
            
        # Poll for status and results
        start_time = time.time()
        result = None
        workflow_completed = False
        
        # First update showing query is being processed
        yield "Processing your query...", "Working on it...", "", conversation_history
        
        while time.time() - start_time < max_wait and not workflow_completed:
            # Check status
            status_response = requests.get(f"{NL2SPARQL_ENDPOINT}/{request_id}/status")
            status_response.raise_for_status()
            status = status_response.json()
            
            # If there's an error in the workflow, return it
            if status.get("has_error"):
                error_msg = status.get("error", "Unknown error occurred")
                raise Exception(f"Workflow error: {error_msg}")
                
            # Generate intermediate reasoning from status
            current_domain = status.get("current_domain", "initializing")
            steps = status.get("steps", [])
            intermediate_reasoning = generate_intermediate_reasoning_from_status(current_domain, steps)
            
            # Yield intermediate results
            yield intermediate_reasoning, f"Processing in {current_domain} domain...", "", conversation_history
            
            # Check if workflow is completed
            workflow_completed = status.get("completed", False)
            if workflow_completed:
                # Get the final result
                result_response = requests.get(f"{NL2SPARQL_ENDPOINT}/{request_id}/result")
                result_response.raise_for_status()
                result = result_response.json()
                break
                
            # Wait before polling again
            time.sleep(POLLING_INTERVAL)
        
        # If we timed out waiting for the result
        if not workflow_completed:
            raise Exception(f"Request timed out after {max_wait} seconds")
        
        # Process final result
        if not result:
            raise Exception("No result returned from the API")
            
        # Extract information from the result
        final_answer = result.get("response", "No answer generated")
        generated_sparql = result.get("sparql_query", "No SPARQL generated")
        
        # Generate final reasoning from the workflow result
        final_reasoning = generate_reasoning_from_result(result)
        
        # Update conversation history with the interaction
        conversation_history.append({"role": "user", "content": user_query})
        conversation_history.append({"role": "assistant", "content": final_answer})
        conversation_history.append({"role": "system", "content": generated_sparql})
        
        return final_reasoning, final_answer, generated_sparql, conversation_history
        
    except Exception as e:
        logger.error(f"Error in process_gradio_query: {e}")
        error_message = f"Error: {str(e)}"
        return error_message, error_message, "", conversation_history


def generate_intermediate_reasoning_from_status(current_domain, steps):
    """Generate intermediate reasoning based on workflow status"""
    reasoning = f"Current stage: {current_domain.upper()} domain\n\n"
    
    if steps:
        reasoning += "Processing steps:\n"
        for step in steps:
            domain = step.get("domain", "unknown")
            task = step.get("task", "unknown")
            status = step.get("status", "unknown")
            start_time = step.get("start_time", "")
            end_time = step.get("end_time", "")
            
            if end_time:
                duration = f"({(float(end_time) - float(start_time)):.2f}s)"
            else:
                duration = "(in progress)"
                
            reasoning += f"- {domain.upper()}: {task} - {status} {duration}\n"
    
    return reasoning


def generate_reasoning_from_result(result):
    """Generate reasoning information from the workflow result"""
    reasoning_parts = []
    
    # Add original and refined query
    original_query = result.get("original_query", "")
    if original_query:
        reasoning_parts.append(f"Original Query: {original_query}")
    
    # Add processing time
    processing_time = result.get("processing_time", 0)
    reasoning_parts.append(f"Total processing time: {processing_time:.2f} seconds")
    
    # Add steps breakdown if available
    steps = result.get("steps", [])
    if steps:
        steps_info = "Processing steps:\n"
        for step in steps:
            domain = step.get("domain", "unknown")
            task = step.get("task", "unknown")
            status = step.get("status", "unknown")
            
            # Calculate duration if available
            start_time = step.get("start_time", "")
            end_time = step.get("end_time", "")
            if start_time and end_time:
                duration = f"({(float(end_time) - float(start_time)):.2f}s)"
            else:
                duration = ""
                
            steps_info += f"- {domain.upper()}: {task} - {status} {duration}\n"
        reasoning_parts.append(steps_info)
    
    # Add additional data if available
    data = result.get("data", {})
    
    # Add entity recognition data
    if "entities" in data:
        entity_info = "Recognized Entities:\n"
        entities = data["entities"]
        if entities:
            for entity_type, entity_list in entities.items():
                entity_info += f"- Type: {entity_type}\n"
                for entity in entity_list:
                    entity_info += f"  - {entity}\n"
        else:
            entity_info += "No entities recognized"
        reasoning_parts.append(entity_info)
    
    # Add SPARQL query
    sparql_query = result.get("sparql_query", "")
    if sparql_query:
        reasoning_parts.append("Generated SPARQL query:\n```\n" + sparql_query + "\n```")
    
    # Join all reasoning parts with double line breaks
    return "\n\n".join(reasoning_parts)


def execute_sparql_query(sparql_query):
    """
    Execute a SPARQL query using the API.
    
    Args:
        sparql_query: The SPARQL query to execute
        
    Returns:
        Formatted query results
    """
    try:
        # Submit the query to the API
        logger.info(f"Executing SPARQL query via API")
        response = requests.post(
            SPARQL_EXEC_ENDPOINT,
            json={"query": sparql_query}
        )
        response.raise_for_status()
        task_id = response.json().get("task_id")
        
        if not task_id:
            raise Exception("No task ID returned from the API")
        
        # Poll for results
        start_time = time.time()
        result = None
        task_completed = False
        
        while time.time() - start_time < MAX_POLLING_TIME and not task_completed:
            response = requests.get(f"{API_BASE_URL}/api/tasks/{task_id}")
            response.raise_for_status()
            
            if response.json().get("status") != "pending":
                result = response.json()
                task_completed = True
                break
                
            # Wait before polling again
            time.sleep(POLLING_INTERVAL)
        
        # If we timed out waiting for the result
        if not task_completed:
            raise Exception(f"SPARQL query timed out after {MAX_POLLING_TIME} seconds")
        
        # Format the results in a readable way
        if result.get("success", False):
            results = result.get("results", {})
            if results.get("head", {}).get("vars"):
                # For SELECT queries
                formatted_result = "Results:\n\n"
                vars = results.get("head", {}).get("vars", [])
                bindings = results.get("results", {}).get("bindings", [])
                
                if bindings:
                    # Format as table
                    formatted_result += " | ".join(vars) + "\n"
                    formatted_result += "-" * (sum(len(v) for v in vars) + 3 * (len(vars) - 1)) + "\n"
                    
                    # Add data rows
                    for binding in bindings:
                        row_values = []
                        for var in vars:
                            if var in binding:
                                row_values.append(str(binding[var].get("value", "")))
                            else:
                                row_values.append("")
                        formatted_result += " | ".join(row_values) + "\n"
                    
                    return formatted_result
                else:
                    return "Query executed successfully, but returned no results."
            elif "boolean" in results:
                # For ASK queries
                return f"Query result: {results.get('boolean', False)}"
            else:
                # For other query types or formats
                return json.dumps(result, indent=2)
        else:
            # Query failed
            return f"Error executing query: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error executing SPARQL: {e}")
        return f"Error executing SPARQL: {e}"


def get_system_health():
    """Get the current health status of the system"""
    try:
        response = requests.get(API_HEALTH_ENDPOINT)
        response.raise_for_status()
        health_data = response.json()
        
        status = health_data.get("status", "unknown")
        services = health_data.get("services", {})
        
        status_html = f"System Status: <span style='color: {'green' if status == 'healthy' else 'red'};'>{status.upper()}</span><br><br>"
        status_html += "Services:<br>"
        
        for service, service_status in services.items():
            color = "green" if service_status == "healthy" else "red"
            status_html += f"- {service}: <span style='color: {color};'>{service_status}</span><br>"
        
        # Try to get NL2SPARQL system stats if available
        try:
            nl2sparql_stats_response = requests.get(f"{API_BASE_URL}/api/nl2sparql/stats")
            if nl2sparql_stats_response.status_code == 200:
                stats = nl2sparql_stats_response.json()
                
                # Add active workflows
                active_workflows = stats.get("active_workflows", 0)
                status_html += f"<br>Active workflows: {active_workflows}<br>"
                
                # Add domain statuses
                if "domains" in stats:
                    status_html += "<br>Domains:<br>"
                    for domain, domain_status in stats["domains"].items():
                        domain_health = domain_status.get("status", "unknown")
                        color = "green" if domain_health == "healthy" else "red"
                        status_html += f"- {domain}: <span style='color: {color};'>{domain_health}</span><br>"
        except Exception:
            # Optional enhancement, ignore if it fails
            pass
            
        return status_html
    except Exception as e:
        logger.error(f"Error getting system health: {e}")
        return f"Error getting system health: {e}"


if __name__ == "__main__":
    with gr.Blocks() as demo:
        gr.Markdown("# NL2SPARQL - Multi-Agent Natural Language to SPARQL Interface")
        
        with gr.Row():
            system_status = gr.HTML("Checking system status...")
        
        with gr.Tab("Query Chat"):
            with gr.Row():
                user_input = gr.Textbox(
                    label="Enter your question", 
                    placeholder="Type your query here...",
                    lines=1
                )
                submit_button = gr.Button("Submit")
                
            with gr.Row():
                reasoning_output = gr.Textbox(
                    label="Processing Details", 
                    lines=15,
                    show_copy_button=True
                )
                
            with gr.Row():
                final_answer_output = gr.Textbox(
                    label="Answer", 
                    lines=3,
                    show_copy_button=True
                )
                
            with gr.Row():
                sparql_output = gr.Textbox(
                    label="Generated SPARQL Query", 
                    lines=5,
                    show_copy_button=True
                )
            
            # Use a Gradio State to maintain the conversation history
            conversation_state = gr.State([])
            
            # Update system status when tab is opened
            refresh_status_button = gr.Button("Refresh System Status")
            refresh_status_button.click(
                get_system_health,
                inputs=[],
                outputs=[system_status]
            )
            
        with gr.Tab("Execute SPARQL"):
            gr.Markdown("Enter a SPARQL query to execute against the knowledge graph")
            
            with gr.Row():
                sparql_input = gr.Textbox(
                    label="SPARQL Query", 
                    placeholder="Type your SPARQL query here...",
                    lines=8,
                    show_copy_button=True
                )
                
            with gr.Row():
                execute_button = gr.Button("Execute Query")
                
            with gr.Row():
                sparql_result = gr.Textbox(
                    label="Query Result", 
                    lines=15,
                    show_copy_button=True
                )
        
        # Set up event handlers
        submit_button.click(
            fn=process_gradio_query,
            inputs=[user_input, conversation_state],
            outputs=[reasoning_output, final_answer_output, sparql_output, conversation_state],
            api_name="process_query"
        )
        
        execute_button.click(
            fn=execute_sparql_query,
            inputs=[sparql_input],
            outputs=[sparql_result],
            api_name="execute_sparql"
        )
        
        demo.load(
            fn=get_system_health,
            inputs=[],
            outputs=[system_status],
        )

    # Launch the Gradio interface
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
    )
