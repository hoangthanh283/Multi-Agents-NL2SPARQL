import json
from typing import Any, Dict

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI


class ValidationAgent:
    """
    Slave agent responsible for validating execution plans.
    Validates that the planning agent's decision to use or not use
    specific SPARQL queries is logical and not a hallucination.
    """
    def __init__(self):
        """
        Initialize the validation agent
        """
        self.agent = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.
        )

        self.num_retry = 1

    def validate_plan(
        self,
        execution_plan: Dict[str, Any],
        query_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate an execution plan for logical errors or hallucinations.
        
        Args:
            execution_plan: The execution plan to validate
            query_context: Context of the original query including entities
            
        Returns:
            Validation result with is_valid flag and feedback
        """
        for i in range(self.num_retry):
            try:
                if not execution_plan or not isinstance(execution_plan, dict):
                    return {
                        "is_valid": False,
                        "validation_type": "structure",
                        "feedback": "Invalid plan format: Plan is empty or not a dict."
                    }
        
                plan = execution_plan.get("steps", [])
                prompt = self._prepare_validation_prompt(query_context["user_query"], plan)
                validation_ans = self.agent.invoke(prompt)
                validation_ans = json.loads(validation_ans.content)
                break
            except Exception as e:
                # print(e, validation_ans.content)
                print(e)
                validation_ans = {
                    "is_valid": False,
                    "validation_type": "structure",
                    "feedback": "Invalid plan format: Plan is empty or not a list."
                }
                continue
        return validation_ans

    def _prepare_validation_prompt(self, user_query, plan):
        if len(plan) <= 0:
            return self._prepare_no_steps_validation_prompt(user_query)
        else:
            return self._prepare_steps_validation_prompt(user_query, plan)

    def _prepare_steps_validation_prompt(self, user_query, plan):
        prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """You are a professional developer with experience in writing SPARQL for ontology file. Your task is to validate the provided plan to transform natural provided user query to SPARQL is valid. Please follow the instruction below:
             - Check each step is correct. If not correct, provide how to improve in short for each step
             - If the step level is complex, if all property is describe in previous steps, it is correct. 
             - Remember **SPARQL support direct computation in the SELECT query**
             - **Remember do not care about the SPARQL detailed**
             - Check if the plan can answer the user query.
             - If plan is valid or can be accepted, the output is {{"is_valid": true, "feedback": []}}
             - If your step is is too vague and does not specify how to check terms, then the output is {{"is_valid": true, "feedback": []}}
             The output format must be following this format:
             {{"is_valid": true or false, "feedback": [
                {{"step": "step query", "feedback": "feedback"}}
             ]}}"""
            ),
            ("user", """**User query:**{user_query}, **Plan**:{plan}""")
        ])
        return prompt.format_messages(user_query=user_query, plan=str(plan))

    def _prepare_no_steps_validation_prompt(self, user_query):
        prompt = ChatPromptTemplate.from_messages([
            ("system",
             """You are a professional developer with experience in writing SPARQL for ontology file. Your task is to check if there exists a plan to transform natural provided user query to SPARQL. Please follow the instruction below:
             - If there exists plan, please provide some suggest for creating plan.
             - If there not exists plan, the output is {{"is_valid": true, "feedback": ""}}
             The output format must be following this format:
             {{"is_valid": true or false, "feedback": "Some suggestion"}}
             """   
            ),
            ("user", "{user_query}")
        ])
        return prompt.format_messages(user_query=user_query)
    