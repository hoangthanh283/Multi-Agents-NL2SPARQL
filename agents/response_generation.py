# agents/response_generation.py
import json
from typing import Any, Dict, List, Optional

import autogen

from config.agent_config import get_agent_config


class ResponseGenerationAgent:
    """
    Slave agent responsible for generating natural language responses.
    Transforms technical API results into conversational, helpful responses.
    """
    
    def __init__(self):
        """Initialize the response generation agent."""
        # Get configuration for response generation agent
        agent_config = get_agent_config("response_generation")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction with the response generation agent
        self.proxy = autogen.UserProxyAgent(
            name="ResponseGenerationProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
    
    def generate_response(
        self, 
        refined_query: str,
        sparql_query: str,
        execution_results: Dict[str, Any]
    ) -> str:
        """
        Generate a natural language response based on execution results.
        
        Args:
            refined_query: The refined user query
            sparql_query: The current SPARQL query
            execution_results: Results from tool execution
            
        Returns:
            Natural language response to the user
        """
        # Prepare the prompt for the LLM
        prompt = self._prepare_response_prompt(refined_query, sparql_query, execution_results)
        
        # Get response from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        response_text = response.summary.strip()
        # If response is empty, provide a fallback
        if not response_text:
            response_text = "I'm sorry, I couldn't generate a proper response based on the information available."
        return response_text
    
    def _prepare_response_prompt(
        self, 
        refined_query: str,
        sparql_query: str,
        execution_results: Dict[str, Any]
    ) -> str:
        """
        Prepare the prompt for the LLM to generate a response.
        
        Args:
            refined_query: The refined user query
            execution_results: Results from tool execution
            
        Returns:
            Complete prompt for the LLM
        """
        # Format the execution results
        results_text = json.dumps(execution_results, indent=2)
        
        # Determine if there were any errors
        has_errors = any("error" in result for result in execution_results.values() if isinstance(result, dict))
        
        # Check if results contain transaction data
        has_transaction = any(
            isinstance(result, dict) and "transaction" in result 
            for result in execution_results.values()
        )
        
        # Construct the complete prompt
        prompt = f"""I need you to create a natural, conversational response to a user's blockchain query.
You will be provided with the user's query and the raw results from blockchain API calls.
Your task is to transform these technical results into a helpful, clear response.

User Query: {refined_query}

Current SPARQL Query: {sparql_query}

Execution Results:
{results_text}

Follow these guidelines:
1. Be conversational and friendly, like you're explaining to a person.
2. Focus on the information most relevant to the user's query.
3. Provide context and explanations for technical terms.
4. Format numbers and data in a human-readable way (e.g., round large numbers, format prices).
5. If the data includes timestamps, convert them to a user-friendly format.
6. If there are errors in the results, acknowledge them honestly but constructively.
7. Avoid technical jargon unless necessary for accuracy.
"""
        
        # Add transaction-specific instructions if needed
        if has_transaction:
            prompt += """
For transaction data:
1. Explain what the transaction will do in simple terms.
2. Highlight important parameters like token amounts, recipients, etc.
3. Remind the user that they would need to approve this transaction in their wallet.
4. DO NOT include the raw transaction data in your response.
"""
        
        # Add error-specific instructions if needed
        if has_errors:
            prompt += """
Since there were errors in the execution:
1. Clearly explain what went wrong in user-friendly terms.
2. Suggest possible solutions or alternatives if appropriate.
3. Be honest about limitations but maintain a helpful tone.
"""
        
        prompt += """
Your response should be complete and self-contained, without references to "the results" or "the data."
Focus on providing value to the user by directly answering their question.
"""
        return prompt
