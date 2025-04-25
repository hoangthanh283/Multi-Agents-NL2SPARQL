import asyncio
import json
import socket
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import aioredis
from prometheus_client import Counter, Gauge

from config.logging_config import get_logger
from utils.circuit_breaker import circuit_breaker

logger = get_logger(__name__, 'service_discovery')

# Metrics
SERVICE_COUNT = Gauge('service_discovery_services_total', 'Total number of registered services', ['service_type'])
INSTANCE_COUNT = Gauge('service_discovery_instances_total', 'Total number of service instances', ['service_type'])
DISCOVERY_ERRORS = Counter('service_discovery_errors_total', 'Number of service discovery errors', ['error_type'])
REGISTRATION_COUNT = Counter('service_discovery_registrations_total', 'Total number of service registrations', ['service_type'])

class ServiceRegistry:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        """Initialize service registry with Redis backend"""
        self.redis_url = redis_url
        self.redis: Optional[aioredis.Redis] = None
        self.service_cache: Dict[str, List[Dict]] = {}
        self.cache_timestamp: Dict[str, float] = {}
        self.cache_ttl = 5  # seconds
        self.instance_id = str(uuid.uuid4())
        self.heartbeat_tasks = {}
        self.service_ttl = 30  # seconds

    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            logger.info("Connected to Redis service registry")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            DISCOVERY_ERRORS.labels(error_type='redis_connection').inc()
            raise

    async def initialize(self):
        """Initialize Redis connection"""
        if not self.redis:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            logger.info("Service registry initialized with Redis connection")

    @circuit_breaker('service_registry')
    async def register_service(self, 
                             service_type: str,
                             host: str,
                             port: int,
                             metadata: Dict = None):
        """Register a service instance"""
        if not self.redis:
            await self.connect()

        instance_data = {
            'id': self.instance_id,
            'host': host,
            'port': port,
            'metadata': metadata or {},
            'last_heartbeat': datetime.now().isoformat(),
            'status': 'healthy'
        }

        key = f"services:{service_type}:{self.instance_id}"
        try:
            await self.redis.hset(key, mapping=instance_data)
            await self.redis.expire(key, 30)  # TTL for instance data
            
            SERVICE_COUNT.labels(service_type=service_type).inc()
            INSTANCE_COUNT.labels(service_type=service_type).inc()
            
            logger.info(f"Registered service {service_type} instance at {host}:{port}")
            
        except Exception as e:
            logger.error(f"Failed to register service: {e}")
            DISCOVERY_ERRORS.labels(error_type='registration').inc()
            raise

    async def deregister_service(self, service_type: str):
        """Deregister a service instance"""
        if not self.redis:
            await self.connect()

        key = f"services:{service_type}:{self.instance_id}"
        try:
            await self.redis.delete(key)
            
            SERVICE_COUNT.labels(service_type=service_type).dec()
            INSTANCE_COUNT.labels(service_type=service_type).dec()
            
            logger.info(f"Deregistered service {service_type} instance {self.instance_id}")
            
        except Exception as e:
            logger.error(f"Failed to deregister service: {e}")
            DISCOVERY_ERRORS.labels(error_type='deregistration').inc()
            raise

    @circuit_breaker('service_discovery')
    async def discover_service(self, service_type: str) -> List[Dict]:
        """Discover all instances of a service type"""
        if not self.redis:
            await self.connect()

        # Check cache first
        now = time.time()
        if (service_type in self.service_cache and 
            now - self.cache_timestamp.get(service_type, 0) < self.cache_ttl):
            return self.service_cache[service_type]

        try:
            pattern = f"services:{service_type}:*"
            keys = await self.redis.keys(pattern)
            
            instances = []
            for key in keys:
                instance_data = await self.redis.hgetall(key)
                if instance_data:
                    # Check if instance is still alive (last heartbeat within 30 seconds)
                    last_heartbeat = datetime.fromisoformat(instance_data['last_heartbeat'])
                    if (datetime.now() - last_heartbeat).total_seconds() <= 30:
                        instances.append(instance_data)

            # Update cache
            self.service_cache[service_type] = instances
            self.cache_timestamp[service_type] = now
            
            return instances
            
        except Exception as e:
            logger.error(f"Failed to discover services: {e}")
            DISCOVERY_ERRORS.labels(error_type='discovery').inc()
            raise

    async def update_status(self, service_type: str, status: str, metadata: Dict = None):
        """Update service instance status and metadata"""
        if not self.redis:
            await self.connect()

        key = f"services:{service_type}:{self.instance_id}"
        try:
            updates = {
                'status': status,
                'last_heartbeat': datetime.now().isoformat()
            }
            if metadata:
                updates['metadata'] = json.dumps(metadata)
                
            await self.redis.hset(key, mapping=updates)
            await self.redis.expire(key, 30)
            
            logger.info(f"Updated status for {service_type} instance to {status}")
            
        except Exception as e:
            logger.error(f"Failed to update service status: {e}")
            DISCOVERY_ERRORS.labels(error_type='status_update').inc()
            raise

    async def start_heartbeat(self, service_type: str, interval: int = 10):
        """Start sending heartbeats"""
        while True:
            try:
                await self.update_status(service_type, 'healthy')
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                await asyncio.sleep(1)  # Brief pause before retry

    async def watch_service_changes(self, service_type: str, callback):
        """Watch for service instance changes"""
        if not self.redis:
            await self.connect()

        pubsub = self.redis.pubsub()
        channel = f"service_changes:{service_type}"
        
        try:
            await pubsub.subscribe(channel)
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    await callback(json.loads(message['data']))
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Service watch failed: {e}")
            DISCOVERY_ERRORS.labels(error_type='watch').inc()
            raise
        finally:
            await pubsub.unsubscribe(channel)

    def get_instance_id(self) -> str:
        """Get the current instance ID"""
        return self.instance_id

    async def get_service_health(self, service_type: str) -> Dict:
        """Get health information for all instances of a service"""
        instances = await self.discover_service(service_type)
        total = len(instances)
        healthy = sum(1 for i in instances if i['status'] == 'healthy')
        
        return {
            'service_type': service_type,
            'total_instances': total,
            'healthy_instances': healthy,
            'health_percentage': (healthy / total * 100) if total > 0 else 0
        }

# Initialize global service registry
service_registry = ServiceRegistry()