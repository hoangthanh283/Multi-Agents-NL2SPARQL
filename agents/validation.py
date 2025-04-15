import json
import re
from typing import Any, Dict, List

import autogen

from config.agent_config import get_agent_config


class ValidationAgent:
    """
    Slave agent responsible for validating execution plans.
    Validates that the planning agent's decision to use or not use
    specific SPARQL queries is logical and not a hallucination.
    """
    
    def __init__(self):
        """Initialize the validation agent."""
        # Get configuration for validation agent
        agent_config = get_agent_config("validation")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction with the validation agent
        self.proxy = autogen.UserProxyAgent(
            name="ValidationProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
    
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
        # Check if plan is empty or None
        if not execution_plan or not isinstance(execution_plan, dict):
            return {
                "is_valid": False,
                "validation_type": "structure",
                "feedback": "Invalid plan format: Plan is empty or not a dictionary."
            }
        
        # Check if the plan has steps
        steps = execution_plan.get("steps", [])
        message = execution_plan.get("message", "")
        
        # If plan explicitly states no steps are needed, accept it but validate the reasoning
        if not steps and message:
            # Validate the reasoning for no steps
            reasoning_validation = self._validate_no_steps_reasoning(message, query_context)
            return reasoning_validation
        
        # If plan has steps, validate them
        if steps:
            # Validate the plan structure and logic
            return self._validate_plan_steps(steps, query_context)
        
        # Empty plan with no explanation is invalid
        return {
            "is_valid": False,
            "validation_type": "completeness",
            "feedback": "Invalid plan: Plan has no steps and no explanation."
        }
    
    def _validate_no_steps_reasoning(
        self, 
        message: str, 
        query_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate the reasoning for a plan with no steps.
        
        Args:
            message: The explanation for why no steps are needed
            query_context: Context of the original query including entities
            
        Returns:
            Validation result
        """
        # Prepare the prompt for the LLM
        prompt = self._prepare_no_steps_validation_prompt(message, query_context)
        
        # Get validation result from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        
        # Extract the validation result from the response
        return self._extract_validation_result(response)
    
    def _validate_plan_steps(
        self, 
        steps: List[Dict[str, Any]], 
        query_context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate the steps in an execution plan.
        
        Args:
            steps: The execution plan steps
            query_context: Context of the original query including entities
            
        Returns:
            Validation result
        """
        # Prepare the prompt for the LLM
        prompt = self._prepare_steps_validation_prompt(steps, query_context)
        
        # Get validation result from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        
        # Extract the validation result from the response
        return self._extract_validation_result(response)
    
    def _prepare_no_steps_validation_prompt(
        self, 
        message: str, 
        query_context: Dict[str, Any]
    ) -> str:
        """
        Prepare a prompt for validating a plan with no steps.
        
        Args:
            message: The explanation for why no steps are needed
            query_context: Context of the original query including entities
            
        Returns:
            Prompt for the LLM
        """
        # Format the query context
        refined_query = query_context.get("refined_query", "")
        
        # Format the mapped entities if available
        entities_text = ""
        if "mapped_entities" in query_context:
            entities_text = json.dumps(query_context["mapped_entities"], indent=2)
        else:
            entities_text = "No mapped entities available."
        
        # Construct the prompt
        prompt = f"""I need you to validate the reasoning for not executing any SPARQL queries for a user request.

User Query: {refined_query}

Mapped Entities:
{entities_text}

The planner decided not to execute any SPARQL queries with the following explanation:
"{message}"

Please validate whether this decision is reasonable or if it might be a hallucination.
Consider:
1. Is the explanation logically sound given the user query?
2. Are there relevant entities mapped that could be used in a SPARQL query?
3. Is the explanation correctly interpreting what the user is asking for?
4. Could the query reasonably be answered with a SPARQL query against a knowledge graph?

Return your validation result in the following JSON format:
```
{{
  "is_valid": true/false,
  "validation_type": "no_steps_reasoning",
  "feedback": "detailed feedback explaining why the decision is valid or invalid"
}}
```
"""
        return prompt
    
    def _prepare_steps_validation_prompt(
        self, 
        steps: List[Dict[str, Any]], 
        query_context: Dict[str, Any]
    ) -> str:
        """
        Prepare a prompt for validating plan steps.
        
        Args:
            steps: The execution plan steps
            query_context: Context of the original query including entities
            
        Returns:
            Prompt for the LLM
        """
        # Format the query context
        refined_query = query_context.get("refined_query", "")
        
        # Format the mapped entities if available
        entities_text = ""
        if "mapped_entities" in query_context:
            entities_text = json.dumps(query_context["mapped_entities"], indent=2)
        else:
            entities_text = "No mapped entities available."
        
        # Format the plan steps
        steps_text = json.dumps(steps, indent=2)
        
        # Construct the prompt
        prompt = f"""I need you to validate an execution plan for answering a SPARQL query.

User Query: {refined_query}

Mapped Entities:
{entities_text}

Execution Plan Steps:
{steps_text}

Please validate whether this plan is reasonable and logical:
1. Are the steps appropriate for answering the query?
2. Are the steps using the available entities correctly?
3. Are there any logical errors or inconsistencies in the steps?
4. Are there any signs of hallucination (e.g., using entities that don't exist)?
5. Are the dependencies between steps correct and logical?

Return your validation result in the following JSON format:
```
{{
  "is_valid": true/false,
  "validation_type": "plan_steps",
  "feedback": "detailed feedback explaining why the plan is valid or invalid",
  "issues": [
    {{
      "step_number": step_number,
      "issue": "description of issue with this step"
    }}
  ] (optional)
}}
```
"""
        return prompt
    
    def _extract_validation_result(self, response) -> Dict[str, Any]:
        """
        Extract the validation result from the LLM response.
        
        Args:
            response: The LLM response
            
        Returns:
            Validation result dictionary
        """
        # Extract the result from the response
        response_text = response.summary.strip()
        try:
            json_match = re.search(r'```(?:json)?\s*({\s*"is_valid".*?})\s*```', response_text, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
                validation_result = json.loads(json_str)
            else:
                # Try to find JSON without the code block
                json_match = re.search(r'({.*"is_valid".*})', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                    validation_result = json.loads(json_str)
                else:
                    # Fallback if no JSON found
                    validation_result = {
                        "is_valid": False,
                        "validation_type": "parsing",
                        "feedback": "Unable to extract validation result."
                    }
        except Exception as e:
            print(f"Error parsing validation result: {e}")
            validation_result = {
                "is_valid": False,
                "validation_type": "parsing",
                "feedback": f"Error parsing validation result: {str(e)}"
            }
        return validation_result
