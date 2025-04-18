import json
from typing import Any, Dict, List, Optional

from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from database.qdrant_client import QdrantClient


class PlanFormulationAgent:
    """
    Slave agent responsible for creating execution plans for SPARQL queries.
    Formulates plans for complex scenarios requiring multiple queries or 
    processing steps.
    """
    def __init__(self):
        """
        Initialize the plan formulation agent
        """
    
        self.agent = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.
        )
    
        self.num_retry = 2

    def _prepare_plan_prompt(self, user_query: str, feedback: Optional[str] = None) -> List[Any]:
        """
        Create prompt for planning based on user query
        
        Args:
            user_query: Natural user query 
        
        Returns:
            Execution plan as a list of dictionary
        """
        prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """You are a professional developer with experience in writing SPARQL for ontology file. Your task is to create a plan to transform the provided natural query to SPARQL. Please follow the detailed instruction below:
        - If a query need to compute or find out the interval of time, divide query into simple natural queries and merge queries in last step. Else please not change the query.
        - Add level for each step: **simple**: can generate query immediately and **complex**: must use previous queries.
        - Add type of SPARQL query for each step: SELECT, ASK, DESCRIBE and CONSTRUCT.
        - Do not create SPARQL query.
        - If can not create plan to transform to SPARQL, the output is []
        The output format must be following this format: 
        [{{"step": "step query", "sparql_type": "SELECT or ASK or DESCRIBE or CONSTRUCT", "level": "simple or complex"}}, ...]"""
            ),
            ("user", "{user_query}{feedback}")
        ])
        if feedback is not None:
            feedback = ". Old plan and feedback: {}. Please improve this plan".format(str(feedback))
        else:
            feedback = ""
        
        return prompt.format_messages(user_query=user_query, feedback=feedback)

    def formulate_plan(
        self, 
        refined_query: str,
        mapped_entities: Optional[Dict[str, Any]] = None, 
        ontology_info: Optional[Dict[str, Any]] = None,
        validation_feedback: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Formulate an execution plan for SPARQL queries
    
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            ontology_info: Information about the ontology structure
            validation_feedback: Optional feedback from validation agent
    
        Returns:
            Execution plan as list
        """
        
        for i in range(self.num_retry):
            try:
                prompt = self._prepare_plan_prompt(refined_query)
                plan = self.agent.invoke(prompt)
                plan = json.loads(plan.content)
                break
            except Exception as e:
                print(e)
                plan = None
                continue 
    
        return plan
