import logging
import os
from typing import Any, Dict

import ray

logger = logging.getLogger(__name__)

def init_ray_cluster(config: Dict[str, Any] = None) -> None:
    """Initialize Ray cluster with configuration"""
    default_config = {
        "address": os.getenv("RAY_ADDRESS", "auto"),
        "runtime_env": {
            "pip": ["rdflib", "SPARQLWrapper", "pandas", "redis"],
        },
        "namespace": "nl2sparql",
        "include_dashboard": True,
        "_system_config": {
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": {
                "type": "filesystem",
                "params": {
                    "directory_path": "/tmp/ray/spill"
                }
            }
        }
    }

    if config:
        default_config.update(config)

    try:
        ray.init(**default_config)
        logger.info("Ray cluster initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Ray cluster: {e}")
        raise

@ray.remote
class DistributedAgent:
    """Base class for distributed agents using Ray"""
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self._state = {}

    def get_state(self) -> Dict[str, Any]:
        """Get agent's current state"""
        return self._state

    def update_state(self, state: Dict[str, Any]) -> None:
        """Update agent's state"""
        self._state.update(state)

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Process a task asynchronously"""
        raise NotImplementedError("Subclasses must implement process_task")