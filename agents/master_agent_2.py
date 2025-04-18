import json
from typing import Any, Dict, List

import autogen

from config.agent_config import get_agent_config
from utils.logging_utils import setup_logging
from loguru import logger

class MasterAgent:
    """
    Master agent that coordinates the Natural Language to SPARQL conversion system.
    Manages the workflow between slave agents and synthesizes responses.
    """
    
    def __init__(self):
        """Initialize the master agent and all slave agents."""
        # Get configuration for master agent
        master_config = get_agent_config("master")
        
        # Initialize master agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=master_config["name"],
            system_message=master_config["system_message"],
            llm_config=master_config["llm_config"]
        )
        
        # Initialize human proxy for interaction
        self.user_proxy = autogen.UserProxyAgent(
            name="UserProxy",
            human_input_mode="ALWAYS",
            is_termination_msg=lambda x: x.get("content", "").rstrip().endswith("TERMINATE"),
        )
        
        # Dictionary to hold all slave agents
        self.slave_agents = {}

    def register_slave_agent(self, agent_type: str, agent_instance):
        """Register a slave agent with the master agent."""
        self.slave_agents[agent_type] = agent_instance

    def process_query(self, user_query: str, conversation_history: List[Dict]) -> str:
        """
        Process a natural language query through the entire agent workflow.
        
        Args:
            user_query: The raw query from the user
            conversation_history: List of previous conversation messages
            
        Returns:
            SPARQL query
        """
        logger.info(f"Processing query: {user_query}")
        result = {"original_query": user_query, "conversation_history": conversation_history}
        try:
            # Step 1: Refine the query
            # conversation_history = None
            refined_query = self._refine_query(user_query, conversation_history)
            result["refined_query"] = refined_query
            logger.info(f"Refined query: {refined_query}")

            # Step 2: Recognize entities in the query
            entities = self._recognize_entities(refined_query)
            if hasattr(entities, 'content'):  # Handle ChatResult object
                entities = {"all_entities": []}  # Fallback if response is invalid
            result["entities"] = entities
            logger.info(f"Recognized {len(entities.get('all_entities', []))} entities")

            # Step 3: Map entities to ontology terms
            mapped_entities = self._map_entities(entities, refined_query)
            if hasattr(mapped_entities, 'content'):  # Handle ChatResult object
                mapped_entities = {
                    "classes": [],
                    "properties": [],
                    "instances": [],
                    "literals": [],
                    "unknown": entities.get("all_entities", [])
                }
            result["mapped_entities"] = mapped_entities
            logger.info(f"Mapped entities to ontology terms")

            # Step 4: Planning 
            plan = self._formulate_plan(refined_query)
            result["plan"] = plan
            logger.info("Create plan for SPARQL query successfully: {}".format(plan))

            # Step 5: Validation the plan
            validation_result = self._validate_plan(plan, refined_query)
            result["validation"] = validation_result
            logger.info("Validation result: {}".format(validation_result))

            if not validation_result.get("is_valid", False):
                feedback = validation_result.get("feedback", None)
                plan = self._formulate_plan(refined_query, feedback)
                result["plan"] = plan
                logger.info("Fixed plan successfully: {}".format(plan))
                validation_result = self._validate_plan(plan, refined_query)
                result["validation"] = validation_result
                logger.info("Validation result: {}".format(validation_result))

            if validation_result.get("is_valid", False):
                response = self._generate_response(plan, mapped_entities)
                result["response"] = response
                logger.info(f"Generated response successfully")
            else:
                error_response = f"I'm sorry, but I couldn't create a valid SPARQL query for your question. {validation_result.get('feedback', '')}"
                result["response"] = error_response
                logger.info(f"Generated error response due to validation failure")
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            result["error"] = str(e)
            result["response"] = f"I'm sorry, but an error occurred while processing your question: {str(e)}"
        return result

    def _refine_query(self, raw_query: str, conversation_history: List[Dict]) -> str:
        """Delegate query refinement to the query refinement slave agent."""
        if "query_refinement" not in self.slave_agents:
            return raw_query  # Fallback to raw query if agent not available
        
        try:
            # Get the refined query from the agent
            refined_query = self.slave_agents["query_refinement"].refine_query(raw_query, conversation_history)
            return refined_query
        except Exception as e:
            logger.error(f"Error refining query: {e}")
            # Return original query if refinement fails
            return raw_query

    def _recognize_entities(self, refined_query: str) -> Dict[str, Any]:
        """Delegate entity recognition to the entity recognition slave agent."""
        if "entity_recognition" not in self.slave_agents:
            return {"all_entities": []}  # Return empty dict if agent not available
        return self.slave_agents["entity_recognition"].recognize_entities(refined_query)

    def _map_entities(self, entities: Dict[str, Any], query_context: str) -> Dict[str, Any]:
        """Delegate entity mapping to the ontology mapping slave agent."""
        if "ontology_mapping" not in self.slave_agents:
            return {
                "classes": [],
                "properties": [],
                "instances": [],
                "literals": [],
                "unknown": entities.get("all_entities", [])
            }
        return self.slave_agents["ontology_mapping"].map_entities(entities, query_context)

    def _formulate_plan(self, refined_query, feedback=None):
        if "plan_formulation" not in self.slave_agents:
            return []

        return self.slave_agents["plan_formulation"].formulate_plan(refined_query, feedback)

    def _validate_plan(self, plan, refined_query):
        if "validation" not in self.slave_agents:
            return {
                "is_valid": True,
                "feedback": []
            }
        return self.slave_agents["validation"].validate_plan(
            execution_plan={
                "steps": plan
            },
            query_context={
                "user_query": refined_query
            }
        )

    def _generate_response(self, plan, mapped_entities=None):
        if "response_generation" not in self.slave_agents:
            return "Sorry I can not answer the question"

        return self.slave_agents["response_generation"].generate(plan, mapped_entities)