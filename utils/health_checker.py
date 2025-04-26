import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
import psutil
from prometheus_client import Counter, Gauge, Histogram

from config.logging_config import get_logger
from slaves.slave_pool_manager import SlavePoolManager
from utils.deployment_manager import deployment_manager

logger = get_logger(__name__, 'health_checker')

# Metrics
SERVICE_HEALTH = Gauge('service_health_status', 'Health status of services', ['service'])
SERVICE_LATENCY = Histogram('service_latency_seconds', 'Service response latency', ['service'])
HEALTH_CHECK_FAILURES = Counter('health_check_failures_total', 'Number of health check failures', ['service'])

class HealthChecker:
    def __init__(self, check_interval: int = 60):
        """
        Initialize the health checker
        
        Args:
            check_interval: Interval in seconds between health checks
        """
        self.check_interval = check_interval
        self.system_metrics = {}
        self.component_status = {}
        self.last_check_time = None
        self.is_running = False
        self.slave_pool_manager = None  # Will be set during initialization
        self.health_endpoints = {
            'api': 'http://localhost:8000/health',
            'graphdb': 'http://localhost:7200/rest/health',
            'redis': 'http://localhost:6379',
            'kafka': 'http://localhost:9092',
            'ray-head': 'http://localhost:8265',
            'prometheus': 'http://localhost:9090/-/healthy',
            'grafana': 'http://localhost:3000/api/health',
            'master-slave': 'http://localhost:8000/api/master/health'
        }
        self.running = False

    def register_slave_pool_manager(self, manager: SlavePoolManager):
        """Register the slave pool manager for health checks"""
        self.slave_pool_manager = manager
        logger.info("Registered slave pool manager with health checker")

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
            elif service == 'master-slave':
                # Special handling for master-slave architecture
                return await self.check_master_slave_health()
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

    async def check_master_slave_health(self) -> Dict:
        """Check health of master-slave architecture components"""
        result = {
            "status": "healthy",
            "message": "Master-slave architecture operational",
            "master": {},
            "slave_pools": {}
        }
        
        # Check if slave pool manager is registered
        if not self.slave_pool_manager:
            result["status"] = "warning"
            result["message"] = "Slave pool manager not registered"
            return result
        
        # Check master health
        try:
            master_health = await self._get_master_health()
            result["master"] = master_health
            
            if master_health.get("status") != "healthy":
                result["status"] = "warning"
                result["message"] = f"Master health issues: {master_health.get('message')}"
        except Exception as e:
            logger.error(f"Error checking master health: {str(e)}")
            result["master"] = {"status": "critical", "message": f"Error: {str(e)}"}
            result["status"] = "critical"
            result["message"] = f"Master health check failed: {str(e)}"
        
        # Check slave pools health
        try:
            slave_pools_health = await self._get_slave_pools_health()
            result["slave_pools"] = slave_pools_health
            
            # Check if any slave pool is critical
            if any(pool.get("status") == "critical" for pool in slave_pools_health.values()):
                result["status"] = "critical"
                result["message"] = "One or more slave pools are in critical state"
            # Check if any slave pool is warning
            elif any(pool.get("status") == "warning" for pool in slave_pools_health.values()) and result["status"] != "critical":
                result["status"] = "warning"
                result["message"] = "One or more slave pools have warnings"
        except Exception as e:
            logger.error(f"Error checking slave pools health: {str(e)}")
            result["slave_pools"] = {"status": "critical", "message": f"Error: {str(e)}"}
            result["status"] = "critical"
            result["message"] = f"Slave pools health check failed: {str(e)}"
            
        return result
    
    async def _get_master_health(self) -> Dict:
        """Get health status of the master component"""
        # This would be implemented to check actual master health
        # For now, we'll return a placeholder healthy status
        return {
            "status": "healthy",
            "message": "Master operational",
            "load": 0.4,  # Example load factor
            "uptime": 3600  # Example uptime in seconds
        }
    
    async def _get_slave_pools_health(self) -> Dict:
        """Get health status of all slave pools"""
        if not self.slave_pool_manager:
            return {"status": "critical", "message": "Slave pool manager not available"}
            
        slave_pools_health = {}
        
        # Get health status for each slave pool
        for domain, pool in self.slave_pool_manager.slave_pools.items():
            try:
                metrics = await pool.get_metrics()
                status = "healthy"
                message = "Slave pool operational"
                
                # Determine status based on metrics
                if metrics.get("active_slaves", 0) == 0:
                    status = "critical"
                    message = "No active slaves in pool"
                elif metrics.get("success_rate", 1.0) < 0.8:
                    status = "warning"
                    message = "Low success rate"
                elif metrics.get("load_factor", 0.0) > 0.9:
                    status = "warning"
                    message = "High load factor"
                
                slave_pools_health[domain] = {
                    "status": status,
                    "message": message,
                    "metrics": metrics
                }
            except Exception as e:
                logger.error(f"Error getting metrics for slave pool {domain}: {str(e)}")
                slave_pools_health[domain] = {
                    "status": "critical",
                    "message": f"Error: {str(e)}"
                }
                
        return slave_pools_health

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