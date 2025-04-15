import json
from typing import Any, Dict, List, Optional

import autogen

from config.agent_config import get_agent_config
from tools.sparql_tools import SPARQLTools
from tools.template_tools import TemplateTools


class PlanFormulationAgent:
    """
    Slave agent responsible for creating execution plans for SPARQL queries.
    Formulates plans for complex scenarios requiring multiple queries or 
    processing steps.
    """
    
    def __init__(self, template_tools: Optional[TemplateTools] = None):
        """
        Initialize the plan formulation agent.
        
        Args:
            template_tools: Template utility tools
        """
        # Get configuration for plan formulation agent
        agent_config = get_agent_config("plan_formulation")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction with the plan formulation agent
        self.proxy = autogen.UserProxyAgent(
            name="PlanFormulationProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
        
        # Initialize template tools
        self.template_tools = template_tools or TemplateTools()
        
        # Initialize SPARQL tools
        self.sparql_tools = SPARQLTools()
    
    def formulate_plan(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, Any], 
        ontology_info: Dict[str, Any],
        validation_feedback: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Formulate an execution plan for SPARQL queries.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            ontology_info: Information about the ontology structure
            validation_feedback: Optional feedback from validation agent
            
        Returns:
            Execution plan as a dictionary
        """
        # Analyze the query and entities to determine required steps
        query_complexity = self._assess_query_complexity(refined_query, mapped_entities)
        
        # For simple queries that can be handled with a single SPARQL query
        if query_complexity == "simple":
            return self._create_simple_plan(refined_query, mapped_entities)
        
        # For complex queries that require multiple SPARQL queries or post-processing
        elif query_complexity == "complex":
            return self._create_complex_plan(
                refined_query, 
                mapped_entities, 
                ontology_info, 
                validation_feedback
            )
        
        # For queries that cannot be answered with SPARQL
        else:
            return {
                "steps": [],
                "message": "Unable to formulate a plan for this query using available tools."
            }
    
    def _assess_query_complexity(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, Any]
    ) -> str:
        """
        Assess the complexity of a query.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            
        Returns:
            Complexity assessment ("simple", "complex", or "unsupported")
        """
        # Check if we have entities that can be mapped to a SPARQL query
        has_classes = len(mapped_entities.get("classes", [])) > 0
        has_properties = len(mapped_entities.get("properties", [])) > 0
        has_instances = len(mapped_entities.get("instances", [])) > 0
        
        # Determine if query requires aggregation, grouping, or other complex features
        requires_aggregation = any(term in refined_query.lower() for term in 
                                 ["count", "average", "sum", "maximum", "minimum", "how many"])
        
        requires_sorting = any(term in refined_query.lower() for term in 
                             ["top", "highest", "lowest", "most", "least", "order", "rank", "sort"])
        
        requires_comparison = any(term in refined_query.lower() for term in 
                                ["more than", "less than", "greater", "smaller", "between"])
        
        requires_multi_hop = any(term in refined_query.lower() for term in 
                               ["related to", "connected to", "linked to", "path between", "indirect"])
        
        # Check for complex queries that need multiple SPARQL queries
        if requires_multi_hop or (requires_aggregation and requires_comparison) or \
           (requires_sorting and requires_aggregation):
            return "complex"
        
        # Check if we have sufficient entities for a simple query
        if has_classes or has_properties or has_instances:
            # Simple SPARQL query should be sufficient
            return "simple"
        
        # Unable to map to SPARQL with available entities
        return "unsupported"
    
    def _create_simple_plan(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a simple execution plan for a single SPARQL query.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            
        Returns:
            Simple execution plan
        """
        # Count entity types to find templates
        entity_counts = {
            "classes": len(mapped_entities.get("classes", [])),
            "properties": len(mapped_entities.get("properties", [])),
            "instances": len(mapped_entities.get("instances", [])),
            "literals": len(mapped_entities.get("literals", []))
        }
        
        # Find templates that match the entity counts
        matching_templates = self.template_tools.find_templates_for_entities(entity_counts)
        
        if not matching_templates:
            # No suitable template found
            return {
                "steps": [],
                "message": "No suitable template found for the given entities."
            }
        
        # Extract keywords from the query for template matching
        query_words = refined_query.lower().split()
        keywords = [word for word in query_words if len(word) > 3]  # Filter out short words
        
        # Find templates matching keywords
        scored_templates = self.template_tools.find_templates_by_keywords(keywords)
        
        # Find the intersection of entity-matching and keyword-matching templates
        best_templates = []
        for scored in scored_templates:
            template = scored["template"]
            if template in matching_templates:
                best_templates.append({
                    "template": template,
                    "score": scored["score"]
                })
        
        if not best_templates:
            # If no intersection, use the first entity-matching template
            template = matching_templates[0]
        else:
            # Use the highest scoring template
            template = best_templates[0]["template"]
        
        # Create a simple plan with one step
        return {
            "steps": [
                {
                    "step_number": 1,
                    "action": "execute_sparql",
                    "template_id": template["id"],
                    "query_type": template["query_type"],
                    "entities": mapped_entities,
                    "depends_on": []
                }
            ],
            "message": f"Using template '{template['name']}' to answer the query."
        }
    
    def _create_complex_plan(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, Any], 
        ontology_info: Dict[str, Any],
        validation_feedback: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a complex execution plan requiring multiple SPARQL queries.
        Uses LLM-based planning for complex scenarios.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            ontology_info: Information about the ontology structure
            validation_feedback: Optional feedback from validation agent
            
        Returns:
            Complex execution plan
        """
        # Prepare the prompt for the LLM
        prompt = self._prepare_plan_prompt(
            refined_query, 
            mapped_entities, 
            ontology_info, 
            validation_feedback
        )
        
        # Get plan from the LLM
        response = self.proxy.initiate_chat(
            self.agent,
            message=prompt
        )
        response_text = response.summary.strip()
        
        # Parse the plan JSON
        try:
            # Find JSON content in response
            start_idx = response_text.find("{")
            end_idx = response_text.rfind("}") + 1
            
            if start_idx >= 0 and end_idx > start_idx:
                plan_json = response_text[start_idx:end_idx]
                plan = json.loads(plan_json)
            else:
                # Fallback to a simple plan structure if no JSON found
                plan = {
                    "steps": [],
                    "message": "Unable to parse complex plan. Please try again."
                }
        except Exception as e:
            print(f"Error parsing plan JSON: {e}")
            plan = {
                "steps": [],
                "message": f"Unable to parse complex plan: {str(e)}. Please try again."
            }
        
        return plan
    
    def _prepare_plan_prompt(
        self, 
        refined_query: str, 
        mapped_entities: Dict[str, Any], 
        ontology_info: Dict[str, Any],
        validation_feedback: Optional[str] = None
    ) -> str:
        """
        Prepare the prompt for the LLM to formulate a complex plan.
        
        Args:
            refined_query: The refined user query
            mapped_entities: Dictionary of mapped ontology entities
            ontology_info: Information about the ontology structure
            validation_feedback: Optional feedback from validation agent
            
        Returns:
            Complete prompt for the LLM
        """
        # Format the entities
        entities_text = json.dumps(mapped_entities, indent=2)
        
        # Format ontology info
        ontology_text = json.dumps(ontology_info, indent=2)
        
        # List available templates
        templates_list = ""
        for i, template in enumerate(self.template_tools.templates, 1):
            templates_list += f"{i}. {template.get('name', 'Unknown')} ({template.get('id', 'unknown_id')}): {template.get('description', '')}\n"
        
        # Construct the complete prompt
        prompt = f"""I need you to create a detailed execution plan for answering a complex SPARQL query.
You will be provided with a user query, mapped ontology entities, and available query templates.
Your task is to formulate a step-by-step plan detailing which SPARQL queries to execute, in what order,
and how to process the results to answer the user's query.

User Query: {refined_query}

Mapped Ontology Entities:
{entities_text}

Ontology Information:
{ontology_text}

Available SPARQL Query Templates:
{templates_list}

Follow these guidelines to create your plan:
1. Analyze which SPARQL queries are required to answer the complete question.
2. Break down complex questions into simpler SPARQL queries if needed.
3. Specify the template to use for each query step.
4. Define dependencies between steps if later steps need results from earlier steps.
5. Specify how to post-process or combine results from multiple queries if needed.
6. Explain the reasoning behind each step.

"""
        
        # Add validation feedback if available
        if validation_feedback:
            prompt += f"""
Previous Validation Feedback:
{validation_feedback}

Please address these issues in your revised plan.
"""
        
        # Add output format instructions
        prompt += """
Return your plan in the following JSON format:
```
{
  "steps": [
    {
      "step_number": 1,
      "action": "execute_sparql",
      "template_id": "template_id_here",
      "query_type": "SELECT/ASK/CONSTRUCT/DESCRIBE",
      "reasoning": "Why this query is needed",
      "entities": {
        "classes": [...],
        "properties": [...],
        "instances": [...],
        "literals": [...]
      },
      "depends_on": []
    },
    {
      "step_number": 2,
      "action": "process_results",
      "operation": "filter/aggregate/transform",
      "reasoning": "Why this processing is needed",
      "depends_on": [1]
    },
    ...
  ],
  "message": "Explanation of the overall plan"
}
```

If the available templates and entities are not suitable for the query, return an empty steps array with an explanatory message.
"""
        
        return prompt
