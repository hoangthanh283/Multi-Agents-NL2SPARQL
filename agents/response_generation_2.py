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
        # Prepare the optimized prompt for the LLM
        prompt = self._prepare_response_prompt(refined_query, sparql_query, execution_results)
        
        # Get response from the LLM with a direct message for faster processing
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
        Prepare an optimized prompt for the LLM to generate a response.
        
        Args:
            refined_query: The refined user query
            sparql_query: The SPARQL query used
            execution_results: Results from tool execution
            
        Returns:
            Optimized prompt for the LLM
        """
        # Format the execution results concisely
        results_text = json.dumps(execution_results, indent=2)
        
        # Check for errors or transaction data
        has_errors = any("error" in str(result).lower() for result in execution_results.values())
        
        # Create a more concise and focused prompt
        prompt = f"""Convert the following technical results into a natural, conversational response:

User Query: {refined_query}
SPARQL Query: {sparql_query}
Results: {results_text}

Guidelines:
1. Be concise and conversational
2. Focus only on information relevant to the query
3. Format numbers and dates in a readable way
4. {"Explain errors clearly and suggest alternatives if possible" if has_errors else "Provide direct answers to the query"}
5. Avoid technical jargon and don't reference "the results" or "the data"
"""
        
        return prompt
