import os
import json
import time
from typing import Any, Dict, List

import autogen
from loguru import logger

from config.agent_config import get_agent_config
from caches.query_cache import ConstructionQueryCache


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
        self.result_cache = ConstructionQueryCache(
            redis_host=os.getenv("REDIS_HOST"),
            redis_port=os.getenv("REDIS_PORT"),
            redis_ttl=os.getenv("REDIS_TTL"),
            es_host=os.getenv("ES_HOST"),
            es_port=os.getenv("ES_PORT"),
            es_index=os.getenv("ES_INDEX"),
            similarity_threshold=0.7
        )   
        self.query_prefix = "cache:query:"

    def register_slave_agent(self, agent_type: str, agent_instance):
        """Register a slave agent with the master agent."""
        self.slave_agents[agent_type] = agent_instance
    
    def _execute_query(self, sparql_query: str) -> Dict[str, Any]:
        """Delegate query execution to the query execution slave agent."""
        if "query_execution" not in self.slave_agents:
            return {
                "success": False,
                "error": "Query execution agent not available"
            }
        return self.slave_agents["query_execution"].execute_query(sparql_query)

    def process_query(self, user_query: str, conversation_history: List[Dict]) -> str:
        """
        Process a natural language query through the entire agent workflow.
        
        Args:
            user_query: The raw query from the user
            conversation_history: List of previous conversation messages
            
        Returns:
            SPARQL query
        """
        process_start_time = time.time()
        logger.info(f"Processing query: {user_query}")
        result = {"original_query": user_query, "conversation_history": conversation_history, "latency": {}}
        try:
            cache_entry = None
            validation_result = {}
            refined_query = None
            
            # Cache check latency
            cache_check_start = time.time()
            if self.result_cache:
                cache_entry = self.result_cache.search(user_query, self.query_prefix)
            result["latency"]["cache_check"] = time.time() - cache_check_start
            logger.info(f"Cache check completed in {result['latency']['cache_check']:.2f}s")

            if not cache_entry:
                # Step 0: Query complexity classification
                complexity_start = time.time()
                is_complex_query = self._classify_complex_query(user_query)
                result["latency"]["complexity_classification"] = time.time() - complexity_start
                logger.info(f"Input query: {user_query} is complex: {is_complex_query} (took {result['latency']['complexity_classification']:.2f}s)")
                
                # Step 1: Refine the query
                refinement_start = time.time()
                refined_query = self._refine_query(user_query, conversation_history) if is_complex_query else user_query
                result["refined_query"] = refined_query
                result["latency"]["query_refinement"] = time.time() - refinement_start
                logger.info(f"Refined query: {refined_query} (took {result['latency']['query_refinement']:.2f}s)")

                # Step 2: Recognize entities in the query
                entity_recognition_start = time.time()
                entities = self._recognize_entities(refined_query)
                if hasattr(entities, 'content'):  # Handle ChatResult object
                    entities = {"all_entities": []}  # Fallback if response is invalid
                result["entities"] = entities
                result["latency"]["entity_recognition"] = time.time() - entity_recognition_start
                logger.info(f"Recognized {len(entities.get('all_entities', []))} entities (took {result['latency']['entity_recognition']:.2f}s)")

                # Step 3: Map entities to ontology terms
                entity_mapping_start = time.time()
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
                result["latency"]["entity_mapping"] = time.time() - entity_mapping_start
                logger.info(f"Mapped entities to ontology terms (took {result['latency']['entity_mapping']:.2f}s)")

                # Step 4: Planning
                planning_start = time.time()
                plan = self._formulate_plan(refined_query) if is_complex_query else [{'step': user_query, 'sparql_type': 'SELECT', 'level': 'simple'}]
                result["plan"] = plan
                result["latency"]["planning"] = time.time() - planning_start
                logger.info(f"Created plan for SPARQL query successfully (took {result['latency']['planning']:.2f}s): {plan}")

                # Step 5: Validation of the plan
                validation_start = time.time()
                if is_complex_query:
                    validation_result = self._validate_plan(plan, refined_query)
                else:
                    validation_result = {"is_valid": True}
                result["validation"] = validation_result
                result["latency"]["initial_validation"] = time.time() - validation_start
                logger.info(f"Validation result (took {result['latency']['initial_validation']:.2f}s): {validation_result}")

                # Step 5b: Fix plan if validation failed
                if not validation_result.get("is_valid", False) and is_complex_query:
                    fix_plan_start = time.time()
                    feedback = validation_result.get("feedback", None)
                    plan = self._formulate_plan(refined_query, feedback)
                    result["plan"] = plan
                    result["latency"]["plan_fixing"] = time.time() - fix_plan_start
                    logger.info(f"Fixed plan (took {result['latency']['plan_fixing']:.2f}s): {plan}")
                    
                    revalidation_start = time.time()
                    validation_result = self._validate_plan(plan, refined_query)
                    result["validation"] = validation_result
                    result["latency"]["revalidation"] = time.time() - revalidation_start
                    logger.info(f"Revalidation result (took {result['latency']['revalidation']:.2f}s): {validation_result}")

            if validation_result.get("is_valid", False) or cache_entry:
                if not cache_entry:
                    response_generation_start = time.time()
                    response = self._generate_response(plan, mapped_entities)
                    result["response"] = response
                    result["latency"]["response_generation"] = time.time() - response_generation_start
                    executed_sparql = result.get("response", [{}])[0].get("query")
                    logger.info(f"Generated response successfully (took {result['latency']['response_generation']:.2f}s)")
                else:
                    executed_sparql = cache_entry.get("sparql", "")
                    refined_query = cache_entry.get("refined_query", "")

                result["sparql"] = executed_sparql
                result["response"] = [{"query": executed_sparql}]
                if not cache_entry:
                    # Step 6: Execute the query if validation passed
                    logger.info("*"*300)
                    logger.info(f"executed_sparql: {result['sparql']}")
                    if executed_sparql:
                        execution_start = time.time()
                        execution_result = self._execute_query(executed_sparql)
                        if hasattr(execution_result, "content"):  # Handle ChatResult object
                            execution_result = {"success": False, "error": "Query execution error occurred"}
                        result["execution"] = execution_result
                        result["latency"]["query_execution"] = time.time() - execution_start
                        logger.info(f"Query execution {'successful' if execution_result.get('success', False) else 'failed'} (took {result['latency']['query_execution']:.2f}s)")

                        # Step 7: Generate response from the execution results
                        final_response_start = time.time()
                        response = self._generate_final_response(refined_query or user_query, result["sparql"], execution_result)
                        
                        # Cache saving latency
                        cache_save_start = time.time()
                        self.result_cache.save(
                            user_query,
                            self.query_prefix,
                            {
                                "answer": response,
                                "sparql": executed_sparql,
                                "refined_query": refined_query,
                                "query_type": "",
                                "template_id": "",
                                "template_based": True,
                                "entities_used": []
                            }
                        )
                        result["latency"]["cache_saving"] = time.time() - cache_save_start
                        
                        if hasattr(response, 'content'):  # Handle ChatResult object
                            response = str(response.content)
                        result["answer"] = response
                        result["latency"]["final_response_generation"] = time.time() - final_response_start
                        logger.info(f"Generated final response (took {result['latency']['final_response_generation']:.2f}s)")
                    else:
                        error_response = f"I'm sorry, but I couldn't create a valid SPARQL query for your question. {validation_result.get('feedback', '')}"
                else:
                    result["answer"] = cache_entry.get("answer", "")
                    logger.info("Using cached response")
            else:
                error_response = f"I'm sorry, but I couldn't create a valid SPARQL query for your question. {validation_result.get('feedback', '')}"
                result["answer"] = error_response
                logger.info(f"Generated error response due to validation failure")
                
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            result["error"] = str(e)
            result["answer"] = f"I'm sorry, but an error occurred while processing your question: {str(e)}"
            
        # Total processing time
        total_time = time.time() - process_start_time
        result["latency"]["total_processing"] = total_time
        logger.info(f"Stage Latency: {result['latency']}")
        logger.info(f"Total query processing time: {total_time:.2f}s")
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

    def _classify_complex_query(self, query: str) -> bool:
        if "query_complexity_classifier" not in self.slave_agents:
            return False

        try:
            is_complex = self.slave_agents["query_complexity_classifier"].is_complex_query(query)
            return is_complex
        except Exception as e:
            logger.error(f"Error classifying query complexity: {e}")
            return False

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

    def _generate_final_response(
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