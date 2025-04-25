import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml
from prometheus_client import Counter

logger = logging.getLogger(__name__)

CONFIG_UPDATES = Counter('config_updates_total', 'Total number of configuration updates', ['component'])

@dataclass
class ServiceConfig:
    service_name: str
    replicas: int
    memory_request: str
    memory_limit: str
    cpu_request: str
    cpu_limit: str
    environment: Dict[str, str]

class ClusterConfiguration:
    def __init__(self):
        self.configs: Dict[str, ServiceConfig] = {}
        self.environment = os.getenv("DEPLOYMENT_ENV", "development")
        self._load_configurations()

    def _load_configurations(self):
        """Load service configurations based on environment"""
        config_path = os.path.join(
            os.path.dirname(__file__),
            f"environments/{self.environment}.yml"
        )
        
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                
            for service_name, config in config_data.get('services', {}).items():
                self.configs[service_name] = ServiceConfig(
                    service_name=service_name,
                    replicas=config.get('replicas', 1),
                    memory_request=config.get('resources', {}).get('requests', {}).get('memory', '256Mi'),
                    memory_limit=config.get('resources', {}).get('limits', {}).get('memory', '512Mi'),
                    cpu_request=config.get('resources', {}).get('requests', {}).get('cpu', '100m'),
                    cpu_limit=config.get('resources', {}).get('limits', {}).get('cpu', '200m'),
                    environment=config.get('environment', {})
                )
                
            logger.info(f"Loaded configuration for environment: {self.environment}")
            
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    def get_service_config(self, service_name: str) -> Optional[ServiceConfig]:
        """Get configuration for a specific service"""
        return self.configs.get(service_name)

    def update_service_config(self, service_name: str, config_updates: Dict[str, Any]) -> None:
        """Update configuration for a specific service"""
        if service_name in self.configs:
            current_config = self.configs[service_name]
            for key, value in config_updates.items():
                if hasattr(current_config, key):
                    setattr(current_config, key, value)
            
            CONFIG_UPDATES.labels(component=service_name).inc()
            logger.info(f"Updated configuration for service: {service_name}")

    def get_all_configs(self) -> Dict[str, ServiceConfig]:
        """Get all service configurations"""
        return self.configs

# Initialize cluster configuration
cluster_config = ClusterConfiguration()