from database.qdrant_client import QdrantClient
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.output_parsers import ResponseSchema
from langchain.output_parsers import StructuredOutputParser
from database.qdrant_client import QdrantClient
from typing import List, Dict, Any, Optional
import json

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
        self.top_k = 6
        self.collection_name = "ontology_embedding"

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

    def _prepare_step_prompt(self, step_query: str, step_query_type: str, previous_queries: Optional[List[Dict[str, Any]]]=None, mapped_entities=None):
        prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """You are a professional developer with experience in writing SPARQL for ontology file. Your task is to transform natural provided query to SPARQL based on the ontology code, query type. Please follow the detailed instruction below:
             - If **query related to computation or compare**, first **convert the var to string by STR** and then **convert to number use xsd:integer or xsd:float**. 
             - Please query number correctly not rdfs:label or rdfs:comment
             - If query need to find the numeric, please convert to get exactly number not reference
             - If can not convert the query to SPARQL, the output is {{"query": "", "step": "query of that step"}}
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
            limit=self.top_k
        ).points

        code_part = ""
        for search_result in search_results:
            code_part += search_result.payload["code"].strip()
        code_part = code_part.strip()
        return code_part