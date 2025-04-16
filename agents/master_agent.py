import json
from typing import Any, Dict, List

import autogen

from config.agent_config import get_agent_config
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

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
        
    def process_query(self, user_query: str, conversation_history: List[Dict]) -> Dict[str, Any]:
        """
        Process a natural language query through the entire agent workflow.
        
        Args:
            user_query: The raw query from the user
            conversation_history: List of previous conversation messages
            
        Returns:
            Dictionary containing the results of the query processing
        """
        logger.info(f"Processing query: {user_query}")
        result = {"original_query": user_query, "conversation_history": conversation_history}
        try:
            # Step 1: Refine the query
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

            # Step 4: Construct SPARQL query
            sparql_query_result = self._construct_sparql(refined_query, mapped_entities)
            if hasattr(sparql_query_result, 'content'):  # Handle ChatResult object
                sparql_query_result = {
                    "sparql": "",
                    "metadata": {
                        "query_type": "SELECT",
                        "entities_used": mapped_entities
                    }
                }
            result["sparql"] = sparql_query_result.get("sparql")
            result["query_metadata"] = sparql_query_result.get("metadata", {})
            logger.info(f"Constructed SPARQL query")
            
            # Step 5: Validate the SPARQL query
            validation_result = self._validate_sparql(
                sparql_query_result.get("sparql", ""),
                sparql_query_result.get("metadata", {})
            )
            if hasattr(validation_result, 'content'):  # Handle ChatResult object
                validation_result = {"is_valid": False, "feedback": "Validation error occurred"}
            result["validation"] = validation_result
            logger.info(f"Validation result: {'Valid' if validation_result.get('is_valid', False) else 'Invalid'}")
            
            # If validation failed, try to fix the query
            if not validation_result.get("is_valid", False):
                logger.info(f"Attempting to fix invalid SPARQL query")
                fixed_query_result = self._fix_sparql(
                    sparql_query_result.get("sparql", ""),
                    sparql_query_result.get("metadata", {}),
                    validation_result
                )
                
                if hasattr(fixed_query_result, 'content'):  # Handle ChatResult object
                    fixed_query_result = {
                        "sparql": "",
                        "metadata": sparql_query_result.get("metadata", {})
                    }
                
                if fixed_query_result.get("sparql"):
                    result["sparql"] = fixed_query_result.get("sparql")
                    result["query_metadata"] = fixed_query_result.get("metadata", {})
                    
                    # Validate the fixed query
                    validation_result = self._validate_sparql(
                        fixed_query_result.get("sparql", ""),
                        fixed_query_result.get("metadata", {})
                    )
                    if hasattr(validation_result, 'content'):  # Handle ChatResult object
                        validation_result = {"is_valid": False, "feedback": "Validation error occurred"}
                    result["validation"] = validation_result
                    logger.info(f"Fixed query validation: {'Valid' if validation_result.get('is_valid', False) else 'Invalid'}")
            
            # Step 6: Execute the query if validation passed
            if validation_result.get("is_valid", False):
                execution_result = self._execute_query(result["sparql"])
                if hasattr(execution_result, 'content'):  # Handle ChatResult object
                    execution_result = {"success": False, "error": "Query execution error occurred"}
                result["execution"] = execution_result
                logger.info(f"Query execution {'successful' if execution_result.get('success', False) else 'failed'}")
                
                # Step 7: Generate response from the execution results
                response = self._generate_response(
                    refined_query,
                    result["sparql"],
                    execution_result
                )
                if hasattr(response, 'content'):  # Handle ChatResult object
                    response = str(response.content)
                result["response"] = response
                logger.info(f"Generated response")
            else:
                # Generate error response if validation failed
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

    def _construct_sparql(
        self, 
        refined_query: str,
        mapped_entities: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Delegate SPARQL construction to the sparql construction slave agent."""
        if "sparql_construction" not in self.slave_agents:
            return {
                "sparql": "",
                "metadata": {
                    "query_type": "SELECT",
                    "entities_used": mapped_entities
                }
            }
        
        result = self.slave_agents["sparql_construction"].construct_query(
            refined_query,
            mapped_entities
        )
        return {
            "sparql": result.get("sparql", ""),
            "metadata": {
                "query_type": result.get("query_type", "SELECT"),
                "template_based": result.get("template_based", False),
                "llm_generated": result.get("llm_generated", True),
                "entities_used": result.get("entities_used", mapped_entities)
            }
        }

    def _validate_sparql(
        self, 
        sparql_query: str,
        query_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Delegate SPARQL validation to the sparql validation slave agent."""
        if "sparql_validation" not in self.slave_agents:
            return {"is_valid": True}  # Assume valid if agent not available
        return self.slave_agents["sparql_validation"].validate_query(
            sparql_query,
            query_metadata
        )

    def _fix_sparql(
        self, 
        sparql_query: str,
        query_metadata: Dict[str, Any],
        validation_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Try to fix an invalid SPARQL query by reconstructing it."""
        if "sparql_construction" not in self.slave_agents:
            return {
                "sparql": "",
                "metadata": query_metadata
            }
        
        # Add validation feedback to query metadata
        query_metadata["validation_feedback"] = validation_result.get("feedback", "")
        
        # Try to reconstruct the query
        result = self.slave_agents["sparql_construction"].construct_query(
            query_metadata.get("original_query", ""),
            query_metadata.get("entities_used", {}),
            validation_result.get("feedback", "")
        )
        return {
            "sparql": result.get("sparql", ""),
            "metadata": {
                "query_type": result.get("query_type", query_metadata.get("query_type", "SELECT")),
                "template_based": result.get("template_based", False),
                "llm_generated": result.get("llm_generated", True),
                "entities_used": result.get("entities_used", query_metadata.get("entities_used", {})),
                "fixed": True,
                "original_sparql": sparql_query
            }
        }
    
    def _execute_query(self, sparql_query: str) -> Dict[str, Any]:
        """Delegate query execution to the query execution slave agent."""
        if "query_execution" not in self.slave_agents:
            return {
                "success": False,
                "error": "Query execution agent not available"
            }
        
        return self.slave_agents["query_execution"].execute_query(sparql_query)
    
    def _generate_response(
        self, 
        refined_query: str,
        sparql_query: str,
        execution_result: Dict[str, Any]
    ) -> str:
        """Delegate response generation to the response generation slave agent."""
        if "response_generation" not in self.slave_agents:
            # Fallback to simple JSON dump if agent not available
            return f"Here are the results: {json.dumps(execution_result, indent=2)}"
        
        return self.slave_agents["response_generation"].generate_response(
            refined_query,
            sparql_query,
            execution_result
        )
    
    def chat(self):
        """Start an interactive chat session with the user."""
        self.user_proxy.initiate_chat(
            self.agent,
            message="Hello! I'm a Natural Language to SPARQL converter. How can I help you query knowledge graphs today?"
        )
