import asyncio
import copy
import json
import logging
from typing import Any, Dict, List

import autogen
import ray
from langchain.agents import Tool
from langchain.memory import ConversationBufferMemory
from loguru import logger
from prometheus_client import Counter, Histogram

from config.agent_config import get_agent_config
from config.ray_config import DistributedAgent
from utils.kafka_handler import QUERY_TOPIC, RESULT_TOPIC, kafka_handler
from utils.monitoring import metrics_logger

logger = logging.getLogger(__name__)

# Metrics
AGENT_REQUESTS = Counter('agent_requests_total', 'Total number of agent requests', ['agent_type'])
AGENT_PROCESSING_TIME = Histogram('agent_processing_seconds', 'Time spent processing requests', ['agent_type'])

@ray.remote
class DistributedMasterAgent(DistributedAgent):
    def __init__(self, agent_id: str):
        super().__init__(agent_id)
        self.memory = ConversationBufferMemory(return_messages=True)
        self.tools: List[Tool] = []
        self.sub_agents = {}
        self._initialize_sub_agents()

    def _initialize_sub_agents(self):
        """Initialize distributed sub-agents"""
        self.sub_agents = {
            'entity_recognition': ray.remote(EntityRecognitionAgent).remote(),
            'ontology_mapping': ray.remote(OntologyMappingAgent).remote(),
            'plan_formulation': ray.remote(PlanFormulationAgent).remote(),
            'sparql_construction': ray.remote(SPARQLConstructionAgent).remote(),
            'validation': ray.remote(ValidationAgent).remote(),
        }

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task using distributed sub-agents"""
        AGENT_REQUESTS.labels(agent_type='master').inc()
        
        with AGENT_PROCESSING_TIME.labels(agent_type='master').time():
            try:
                # Send task to Kafka for logging
                kafka_handler.produce_message(QUERY_TOPIC, {
                    'task_id': task.get('id'),
                    'query': task.get('query'),
                    'timestamp': task.get('timestamp')
                })

                # Process task using Ray actors
                result = await self._process_distributed(task)

                # Log metrics
                metrics_logger.log_metrics({
                    'task_id': task.get('id'),
                    'processing_time': result.get('processing_time'),
                    'success': result.get('success', False)
                })

                # Send result to Kafka
                kafka_handler.produce_message(RESULT_TOPIC, result)

                return result

            except Exception as e:
                error_msg = f"Error processing task: {str(e)}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }

    async def _process_distributed(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process task using distributed sub-agents"""
        try:
            # Entity recognition
            entities_future = self.sub_agents['entity_recognition'].process_task.remote(task)
            entities = await ray.get(entities_future)

            # Ontology mapping
            mapping_task = {**task, 'entities': entities}
            mapping_future = self.sub_agents['ontology_mapping'].process_task.remote(mapping_task)
            mappings = await ray.get(mapping_future)

            # Plan formulation
            plan_task = {**mapping_task, 'mappings': mappings}
            plan_future = self.sub_agents['plan_formulation'].process_task.remote(plan_task)
            plan = await ray.get(plan_future)

            # SPARQL construction
            sparql_task = {**plan_task, 'plan': plan}
            sparql_future = self.sub_agents['sparql_construction'].process_task.remote(sparql_task)
            sparql = await ray.get(sparql_future)

            # Validation
            validation_task = {**sparql_task, 'sparql': sparql}
            validation_future = self.sub_agents['validation'].process_task.remote(validation_task)
            validation = await ray.get(validation_future)

            if not validation.get('valid', False):
                return {
                    'success': False,
                    'error': validation.get('error', 'Invalid SPARQL query')
                }

            return {
                'success': True,
                'query': task.get('query'),
                'sparql': sparql,
                'entities': entities,
                'mappings': mappings,
                'plan': plan,
                'validation': validation
            }

        except Exception as e:
            logger.error(f"Error in distributed processing: {e}")
            raise

    async def update_tools(self, tools: List[Tool]) -> None:
        """Update available tools"""
        self.tools = tools
        # Distribute tools to sub-agents as needed
        tool_updates = []
        for agent in self.sub_agents.values():
            tool_updates.append(agent.update_tools.remote(tools))
        await ray.get(tool_updates)

    def get_conversation_history(self) -> List[Dict[str, Any]]:
        """Get conversation history"""
        return self.memory.chat_memory.messages