import json
import os

import gradio as gr

from agents.query_execution import QueryExecutionAgent
from main import (create_master_agent, initialize_databases, initialize_models,
                  process_query)
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)
GRAPHDB_URL = os.getenv("GRAPHDB_URL")
GRAPHDB_REPO_ID = os.getenv("GRAPHDB_REPOSITORY")
GRAPHDB_ENDPOINT = os.path.join(GRAPHDB_URL, GRAPHDB_REPO_ID)

qdrant_client = initialize_databases()
bi_encoder, _, entity_recognition_model = initialize_models()
master_agent = create_master_agent(qdrant_client, bi_encoder, entity_recognition_model)

# Initialize the query execution agent to run SPARQL against GraphDB.
query_execution_agent = QueryExecutionAgent(endpoint_url=GRAPHDB_ENDPOINT)


def process_gradio_query(user_query, conversation_history):
    """
    Process the natural language query and update conversation history.
    Returns: intermediate reasoning, final response, generated SPARQL query, updated history.
    """
    # Process the query using the master agent.
    # result = master_agent.process_query(user_query, conversation_history)
    result = process_query(master_agent, user_query, conversation_history)

    # Generate intermediate reasoning from the result object
    intermediate_reasoning = generate_intermediate_reasoning(result)
    
    # Extract the final response and SPARQL query.
    final_answer = result.get("answer", "No final response.")
    # generated_sparql = result.get("sparql", "No SPARQL query generated.")
    generated_sparql = result["response"][-1].get("query", "No SPARQL generated")
    logger.info(f"\n GRADIO SPARQL Query: {generated_sparql}")

    # Update conversation history with the interaction.
    conversation_history.append({"role": "user", "content": user_query})
    conversation_history.append({"role": "assistant", "content": generated_sparql})
    return intermediate_reasoning, final_answer, generated_sparql, conversation_history


def generate_intermediate_reasoning(result):
    """
    Extract and format the intermediate reasoning steps from the result object.
    """
    reasoning_parts = []
    
    # Add refined query
    if "refined_query" in result:
        reasoning_parts.append(f"Refined Query: {result['refined_query']}")
    
    # Add entity recognition results
    if "entities" in result:
        entities_info = "Recognized Entities:\n"
        all_entities = result.get("entities", {}).get("all_entities", [])
        if all_entities:
            for entity in all_entities:
                entity_text = entity.get("text", entity.get("entity_text", "Unknown"))
                entity_type = entity.get("type", entity.get("entity_type", "Unknown"))
                entities_info += f"- {entity_text} ({entity_type})\n"
        else:
            entities_info += "No entities recognized."
        reasoning_parts.append(entities_info)
    
    # Add mapped entities
    if "mapped_entities" in result and result["mapped_entities"]:
        mapped_entities = result["mapped_entities"]
        mapped_info = "Mapped Ontology Terms:\n"
        
        # Format classes
        if mapped_entities.get("classes"):
            mapped_info += "Classes:\n"
            for cls in mapped_entities["classes"]:
                label = cls.get("label", cls.get("text", "Unknown"))
                uri = cls.get("uri", "No URI")
                mapped_info += f"- {label} ({uri})\n"
                
        # Format properties
        if mapped_entities.get("properties"):
            mapped_info += "Properties:\n"
            for prop in mapped_entities["properties"]:
                label = prop.get("label", prop.get("text", "Unknown"))
                uri = prop.get("uri", "No URI")
                mapped_info += f"- {label} ({uri})\n"
                
        # Format instances
        if mapped_entities.get("instances"):
            mapped_info += "Instances:\n"
            for inst in mapped_entities["instances"]:
                label = inst.get("label", inst.get("text", "Unknown"))
                uri = inst.get("uri", "No URI")
                mapped_info += f"- {label} ({uri})\n"
        reasoning_parts.append(mapped_info)

    # Add template/query type information
    if "query_metadata" in result:
        metadata = result["query_metadata"]
        query_info = "Query Information:\n"
        query_info += f"- Query Type: {metadata.get('query_type', 'Unknown')}\n"
        query_info += f"- Template-based: {metadata.get('template_based', False)}\n"
        
        reasoning_parts.append(query_info)
    
    # Add validation results
    if "validation" in result:
        validation = result["validation"]
        validation_info = "Validation Results:\n"
        validation_info += f"- Valid: {validation.get('is_valid', False)}\n"
        if "feedback" in validation:
            validation_info += f"- Feedback: {validation['feedback']}\n"
        
        reasoning_parts.append(validation_info)
    
    # Add execution results if available
    if "execution" in result and result["execution"].get("success", False):
        execution_info = "Execution Results:\n"
        execution_info += "- Query executed successfully\n"
        
        # Add basic result counts if available
        results = result["execution"].get("results", {})
        if "count" in results:
            execution_info += f"- Returned {results['count']} results\n"
        elif "rows" in results:
            execution_info += f"- Returned {len(results['rows'])} rows\n"
        
        reasoning_parts.append(execution_info)
    
    # Join all reasoning parts with double line breaks
    return "\n\n".join(reasoning_parts)


def execute_sparql_query(sparql_query):
    """
    Execute a SPARQL query on GraphDB using the preconfigured QueryExecutionAgent.
    """
    try:
        # Execute the query. The agent can return raw results or formatted data.
        execution_result = query_execution_agent.execute_query(sparql_query)
        
        # Format the results in a readable way
        if execution_result.get("success", False):
            results = execution_result.get("results", {})
            if results.get("format") == "bindings":
                # For SELECT queries
                formatted_result = "Results:\n\n"
                rows = results.get("rows", [])
                
                if rows:
                    # Get all variables
                    variables = results.get("variables", [])
                    
                    # Format as table
                    formatted_result += " | ".join(variables) + "\n"
                    formatted_result += "-" * (sum(len(v) for v in variables) + 3 * (len(variables) - 1)) + "\n"
                    
                    # Add data rows
                    for row in rows:
                        row_values = []
                        for var in variables:
                            if var in row and row[var]:
                                row_values.append(str(row[var].get("value", "")))
                            else:
                                row_values.append("")
                        formatted_result += " | ".join(row_values) + "\n"
                    
                    return formatted_result
                else:
                    return "Query executed successfully, but returned no results."
            elif results.get("format") == "boolean":
                # For ASK queries
                return f"Query result: {results.get('boolean', False)}"
            else:
                # For other query types or formats
                return json.dumps(execution_result, indent=2)
        else:
            # Query failed
            return f"Error executing query: {execution_result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error executing SPARQL: {e}")
        return f"Error executing SPARQL: {e}"
    

if __name__ == "__main__":
    with gr.Blocks() as demo:
        gr.Markdown("# NL Query to SPARQL")
        
        with gr.Tab("Query Chat"):
            with gr.Row():
                user_input = gr.Textbox(label="Enter your question", placeholder="Type your query here...", lines=1)
                submit_button = gr.Button("Submit")
            with gr.Row():
                reasoning_output = gr.Textbox(label="Intermediate Reasoning", lines=10)
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
            sparql_result = gr.Textbox(label="Query Result", lines=10)
            execute_button.click(
                execute_sparql_query,
                inputs=[sparql_input],
                outputs=[sparql_result]
            )

    demo.launch()
