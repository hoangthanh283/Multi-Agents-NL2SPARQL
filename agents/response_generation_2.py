import json
from typing import Any, Dict, List, Optional

import autogen
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from config.agent_config import get_agent_config
from database.qdrant_client import QdrantClient
from utils.constants import QDRANT_SEARCH_THRESHOLD, TOP_K_DRANT_QUERIES
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)


class ResponseGenerationAgent:
    def __init__(self):
        """
        Initialize the response generation agent
        """
        self.agent = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.
        )
        self.qdrant_client = QdrantClient()
        self.num_retry = 2
        self.top_k = TOP_K_DRANT_QUERIES
        self.collection_name = "ontology_embedding"
        # Initialize proxy agent for interaction with the response generation agent
        self.proxy = autogen.UserProxyAgent(
            name="ResponseGenerationProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
        # Initialize the agent with AutoGen
        agent_config = get_agent_config("response_generation")
        self.autogen_agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )

    def generate(self, steps: List[Dict[str, Any]], mapped_entities):
        if not steps:
            return "I'm sorry, I couldn't generate a proper response based on the information avalable"

        previous_queries = []
        for step in steps:
            prompt = self._prepare_step_prompt(
                step_query=step["step"],
                step_query_type=step["sparql_type"],
                previous_queries=previous_queries if step["level"] == "complex" else None,
                
            ) 
            step_query = self.agent.invoke(prompt)
            step_query = json.loads(step_query.content)
            previous_queries.append(step_query)
        return previous_queries

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
            self.autogen_agent,
            message=prompt
        )
        response_text = response.summary.strip()
        # If response is empty, provide a fallback
        if not response_text:
            response_text = "I'm sorry, I couldn't generate a proper response based on the information available."
        return response_text

    def _prepare_step_prompt(self, step_query: str, step_query_type: str, previous_queries: Optional[List[Dict[str, Any]]]=None, mapped_entities=None):
        prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """You are a professional developer with experience in writing SPARQL for ontology file. Your task is to transform natural provided query to SPARQL based on the ontology code, query type and combine with previous SPARQL code (if has). Please follow the detailed instruction below:
             - If **query related to computation or compare**, first **convert the var to string by STR** and then **convert to number use xsd:integer or xsd:float**. For example: xsd:integet(STR(?a))
             - Please query number correctly not rdfs:label or rdfs:comment
             - If query need to find the numeric, please convert to get exactly number not reference
             - If can not convert the query to SPARQL, the output is {{"query": "", "step": "query of that step"}}
             - Add PREFIX to the SPARQL query for xsd, rdfs and other (if necessary)
             **Output SPARQL type**:
             {sparql_type}
             **Provided query**:
             {query}
             {mapped_entities}
             **Ontology code**:
             {ontology_code}
             {sparql_code}
             The output format must be in the following format:
             {{"query": "SPARQL query", "step": "query of that step"}}
             """
            )
        ])
        if previous_queries is None:
            sparql_code = ""
        else:
            sparql_code = "**SparQL code**:\n" + str(previous_queries)

        if mapped_entities is None:
            mapped_entities = ""
        else:
            mapped_entities = "**Entities**:\n" + str(mapped_entities)

        ontology_code = self._get_code_part(step_query)

        return prompt.format_messages(
            sparql_type=step_query_type,
            query=step_query,
            ontology_code=ontology_code,
            sparql_code=sparql_code,
            mapped_entities=mapped_entities
        )

    def _get_code_part(self, step_query: str) -> str:
        """
        Search code part in ontology for step query

        Args:
            step_query: str: query for each step

        Returns:
            Part of ontology related to step query
        """
        search_results = self.qdrant_client.client.query_points(
            collection_name=self.collection_name,
            query=self.qdrant_client.default_model.encode(step_query),
            score_threshold=QDRANT_SEARCH_THRESHOLD,
            limit=self.top_k
        ).points

        # If we could not find any match, then use the top-2 matches that can be find.
        if not search_results:
            search_results = self.qdrant_client.client.query_points(
                collection_name=self.collection_name,
                query=self.qdrant_client.default_model.encode(step_query),
                limit=2
            ).points

        code_part = ""
        logger.info("#"*300)
        for search_result in search_results:
            logger.info(f"search_result: {search_result}")
            code_part += search_result.payload["code"].strip()
        code_part = code_part.strip()
        return code_part

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

CUrrent SPARQL Query: {sparql_query}

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
