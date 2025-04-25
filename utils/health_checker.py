import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
from prometheus_client import Counter, Gauge, Histogram

from config.logging_config import get_logger
from utils.deployment_manager import deployment_manager

logger = get_logger(__name__, 'health_checker')

# Metrics
SERVICE_HEALTH = Gauge('service_health_status', 'Health status of services', ['service'])
SERVICE_LATENCY = Histogram('service_latency_seconds', 'Service response latency', ['service'])
HEALTH_CHECK_FAILURES = Counter('health_check_failures_total', 'Number of health check failures', ['service'])

class HealthChecker:
    def __init__(self, check_interval: int = 60):
        self.check_interval = check_interval
        self.health_endpoints = {
            'api': 'http://localhost:8000/health',
            'graphdb': 'http://localhost:7200/rest/health',
            'redis': 'http://localhost:6379',
            'kafka': 'http://localhost:9092',
            'ray-head': 'http://localhost:8265',
            'prometheus': 'http://localhost:9090/-/healthy',
            'grafana': 'http://localhost:3000/api/health'
        }
        self.running = False

    async def start_monitoring(self):
        """Start health monitoring for all services"""
        self.running = True
        while self.running:
            try:
                await self.check_all_services()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")

    async def stop_monitoring(self):
        """Stop health monitoring"""
        self.running = False

    async def check_all_services(self):
        """Check health of all services"""
        async with aiohttp.ClientSession() as session:
            tasks = []
            for service, endpoint in self.health_endpoints.items():
                tasks.append(self.check_service_health(session, service, endpoint))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results and take action if needed
            for service, result in zip(self.health_endpoints.keys(), results):
                if isinstance(result, Exception):
                    logger.error(f"Health check failed for {service}: {result}")
                    await self.handle_service_failure(service)
                else:
                    SERVICE_HEALTH.labels(service=service).set(1 if result['healthy'] else 0)
                    if 'latency' in result:
                        SERVICE_LATENCY.labels(service=service).observe(result['latency'])

    async def check_service_health(self, 
                                 session: aiohttp.ClientSession,
                                 service: str,
                                 endpoint: str) -> Dict:
        """Check health of a specific service"""
        start_time = time.time()
        try:
            if service == 'redis':
                # Special handling for Redis
                return await self.check_redis_health()
            elif service == 'kafka':
                # Special handling for Kafka
                return await self.check_kafka_health()
            else:
                # HTTP health check
                async with session.get(endpoint) as response:
                    latency = time.time() - start_time
                    is_healthy = response.status == 200
                    
                    if not is_healthy:
                        HEALTH_CHECK_FAILURES.labels(service=service).inc()
                    
                    return {
                        'healthy': is_healthy,
                        'latency': latency,
                        'status_code': response.status,
                        'timestamp': datetime.utcnow().isoformat()
                    }

        except Exception as e:
            HEALTH_CHECK_FAILURES.labels(service=service).inc()
            logger.error(f"Health check error for {service}: {e}")
            raise

    async def check_redis_health(self) -> Dict:
        """Check Redis health using redis-cli ping"""
        start_time = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                'redis-cli', 'ping',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            latency = time.time() - start_time
            is_healthy = stdout.decode().strip() == 'PONG'
            
            if not is_healthy:
                HEALTH_CHECK_FAILURES.labels(service='redis').inc()
            
            return {
                'healthy': is_healthy,
                'latency': latency,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            HEALTH_CHECK_FAILURES.labels(service='redis').inc()
            raise

    async def check_kafka_health(self) -> Dict:
        """Check Kafka health"""
        start_time = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                'kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            latency = time.time() - start_time
            is_healthy = proc.returncode == 0
            
            if not is_healthy:
                HEALTH_CHECK_FAILURES.labels(service='kafka').inc()
            
            return {
                'healthy': is_healthy,
                'latency': latency,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            HEALTH_CHECK_FAILURES.labels(service='kafka').inc()
            raise

    async def handle_service_failure(self, service: str):
        """Handle service failure"""
        try:
            # Get current service status
            status = deployment_manager.get_service_status(service)
            
            if status['available'] < status['desired']:
                logger.warning(f"Service {service} has reduced availability. Attempting recovery...")
                
                # Try to restart the service
                success = deployment_manager.deploy_service(service)
                
                if not success:
                    logger.error(f"Failed to recover service {service}")
                    # Optionally trigger alerts or notifications here
                else:
                    logger.info(f"Successfully recovered service {service}")

        except Exception as e:
            logger.error(f"Error handling failure for service {service}: {e}")

# Initialize health checker
health_checker = HealthChecker()