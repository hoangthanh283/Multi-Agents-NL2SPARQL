import logging
import os
import subprocess
from typing import Dict, List, Optional

import yaml
from prometheus_client import Counter, Gauge

from config.cluster_config import cluster_config
from config.logging_config import get_logger

logger = get_logger(__name__, 'deployment')

# Metrics
DEPLOYMENT_OPERATIONS = Counter('deployment_operations_total', 'Total number of deployment operations', ['operation', 'status'])
SERVICE_REPLICAS = Gauge('service_replicas', 'Number of replicas per service', ['service'])

class DeploymentManager:
    def __init__(self):
        self.environment = os.getenv("DEPLOYMENT_ENV", "development")
        self.kubernetes_enabled = os.getenv("USE_KUBERNETES", "false").lower() == "true"

    def deploy_service(self, service_name: str) -> bool:
        """Deploy or update a service"""
        try:
            config = cluster_config.get_service_config(service_name)
            if not config:
                logger.error(f"No configuration found for service: {service_name}")
                return False

            if self.kubernetes_enabled:
                success = self._deploy_to_kubernetes(service_name, config)
            else:
                success = self._deploy_to_docker_compose(service_name, config)

            if success:
                SERVICE_REPLICAS.labels(service=service_name).set(config.replicas)
                DEPLOYMENT_OPERATIONS.labels(operation='deploy', status='success').inc()
                logger.info(f"Successfully deployed {service_name}")
            else:
                DEPLOYMENT_OPERATIONS.labels(operation='deploy', status='failure').inc()
                logger.error(f"Failed to deploy {service_name}")

            return success

        except Exception as e:
            logger.error(f"Error deploying service {service_name}: {e}")
            DEPLOYMENT_OPERATIONS.labels(operation='deploy', status='failure').inc()
            return False

    def _deploy_to_kubernetes(self, service_name: str, config: Dict) -> bool:
        """Deploy service to Kubernetes"""
        try:
            # Apply Kubernetes configs
            k8s_files = [
                f"k8s/{service_name}-deployment.yml",
                f"k8s/{service_name}-service.yml"
            ]

            for k8s_file in k8s_files:
                if os.path.exists(k8s_file):
                    result = subprocess.run(
                        ["kubectl", "apply", "-f", k8s_file],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode != 0:
                        logger.error(f"Failed to apply {k8s_file}: {result.stderr}")
                        return False

            # Scale deployment if needed
            subprocess.run([
                "kubectl", "scale", "deployment",
                service_name, f"--replicas={config.replicas}"
            ])

            return True

        except Exception as e:
            logger.error(f"Error in Kubernetes deployment: {e}")
            return False

    def _deploy_to_docker_compose(self, service_name: str, config: Dict) -> bool:
        """Deploy service using Docker Compose"""
        try:
            # Scale service
            result = subprocess.run([
                "docker-compose",
                "up",
                "-d",
                "--scale", f"{service_name}={config.replicas}",
                service_name
            ], capture_output=True, text=True)

            return result.returncode == 0

        except Exception as e:
            logger.error(f"Error in Docker Compose deployment: {e}")
            return False

    def rollback_service(self, service_name: str) -> bool:
        """Rollback a service to its previous version"""
        try:
            if self.kubernetes_enabled:
                result = subprocess.run([
                    "kubectl", "rollout", "undo",
                    "deployment", service_name
                ], capture_output=True, text=True)
            else:
                # For Docker Compose, we'll restart with previous image
                result = subprocess.run([
                    "docker-compose", "up", "-d",
                    "--force-recreate", service_name
                ], capture_output=True, text=True)

            success = result.returncode == 0
            status = 'success' if success else 'failure'
            DEPLOYMENT_OPERATIONS.labels(operation='rollback', status=status).inc()

            return success

        except Exception as e:
            logger.error(f"Error rolling back service {service_name}: {e}")
            DEPLOYMENT_OPERATIONS.labels(operation='rollback', status='failure').inc()
            return False

    def get_service_status(self, service_name: str) -> Dict:
        """Get current status of a service"""
        try:
            if self.kubernetes_enabled:
                result = subprocess.run([
                    "kubectl", "get", "deployment",
                    service_name, "-o", "json"
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    status = yaml.safe_load(result.stdout)
                    return {
                        'available': status.get('status', {}).get('availableReplicas', 0),
                        'desired': status.get('status', {}).get('replicas', 0),
                        'up_to_date': status.get('status', {}).get('updatedReplicas', 0)
                    }
            else:
                result = subprocess.run([
                    "docker-compose", "ps", service_name,
                    "--format", "json"
                ], capture_output=True, text=True)
                
                if result.returncode == 0:
                    containers = yaml.safe_load(result.stdout)
                    return {
                        'available': len([c for c in containers if c['State'] == 'running']),
                        'desired': cluster_config.get_service_config(service_name).replicas,
                        'up_to_date': len(containers)
                    }

        except Exception as e:
            logger.error(f"Error getting status for service {service_name}: {e}")
            
        return {'available': 0, 'desired': 0, 'up_to_date': 0}

# Initialize deployment manager
deployment_manager = DeploymentManager()