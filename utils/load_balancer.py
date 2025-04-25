import asyncio
import random
import time
from collections import defaultdict
from typing import Dict, List, Optional

from prometheus_client import Counter, Gauge, Histogram

from config.logging_config import get_logger
from utils.circuit_breaker import circuit_breaker
from utils.service_discovery import service_registry

logger = get_logger(__name__, 'load_balancer')

# Metrics
REQUEST_COUNT = Counter('load_balancer_requests_total', 'Total number of load balanced requests', ['service_type'])
LATENCY = Histogram('load_balancer_request_duration_seconds', 'Request duration', ['service_type'])
ACTIVE_CONNECTIONS = Gauge('load_balancer_active_connections', 'Number of active connections', ['service_type'])

class LoadBalancer:
    def __init__(self):
        self.active_connections = defaultdict(int)
        self.last_used_index = defaultdict(int)
        self.service_weights: Dict[str, Dict[str, float]] = defaultdict(dict)

    @circuit_breaker('load_balancer')
    async def get_next_instance(self, service_type: str, strategy: str = 'round_robin') -> Optional[Dict]:
        """Get next service instance based on load balancing strategy"""
        instances = await service_registry.discover_service(service_type)
        
        if not instances:
            logger.warning(f"No instances available for service {service_type}")
            return None

        if strategy == 'round_robin':
            instance = await self._round_robin_select(service_type, instances)
        elif strategy == 'least_connections':
            instance = await self._least_connections_select(service_type, instances)
        elif strategy == 'weighted_random':
            instance = await self._weighted_random_select(service_type, instances)
        else:
            instance = await self._round_robin_select(service_type, instances)

        REQUEST_COUNT.labels(service_type=service_type).inc()
        return instance

    async def _round_robin_select(self, service_type: str, instances: List[Dict]) -> Dict:
        """Round-robin instance selection"""
        index = self.last_used_index[service_type]
        self.last_used_index[service_type] = (index + 1) % len(instances)
        return instances[index]

    async def _least_connections_select(self, service_type: str, instances: List[Dict]) -> Dict:
        """Least connections instance selection"""
        min_connections = float('inf')
        selected_instance = None

        for instance in instances:
            instance_id = instance['id']
            connections = self.active_connections[f"{service_type}:{instance_id}"]
            
            if connections < min_connections:
                min_connections = connections
                selected_instance = instance

        return selected_instance or instances[0]

    async def _weighted_random_select(self, service_type: str, instances: List[Dict]) -> Dict:
        """Weighted random instance selection"""
        total_weight = 0
        weights = []

        for instance in instances:
            instance_id = instance['id']
            # Get weight from metadata or use default
            weight = float(instance.get('metadata', {}).get('weight', 1.0))
            # Adjust weight based on current connections
            connections = self.active_connections[f"{service_type}:{instance_id}"]
            adjusted_weight = weight / (connections + 1)
            
            weights.append(adjusted_weight)
            total_weight += adjusted_weight

        if total_weight == 0:
            return random.choice(instances)

        r = random.uniform(0, total_weight)
        cumulative_weight = 0

        for i, instance in enumerate(instances):
            cumulative_weight += weights[i]
            if r <= cumulative_weight:
                return instance

        return instances[-1]

    async def update_instance_weight(self, service_type: str, instance_id: str, weight: float):
        """Update weight for weighted random selection"""
        self.service_weights[service_type][instance_id] = weight

    async def start_connection(self, service_type: str, instance_id: str):
        """Track new connection to an instance"""
        key = f"{service_type}:{instance_id}"
        self.active_connections[key] += 1
        ACTIVE_CONNECTIONS.labels(service_type=service_type).inc()

    async def end_connection(self, service_type: str, instance_id: str):
        """Track ended connection to an instance"""
        key = f"{service_type}:{instance_id}"
        if self.active_connections[key] > 0:
            self.active_connections[key] -= 1
            ACTIVE_CONNECTIONS.labels(service_type=service_type).dec()

    @circuit_breaker('load_balancer_request')
    async def route_request(self, service_type: str, request_func, *args, **kwargs):
        """Route a request through the load balancer"""
        start_time = time.time()
        instance = await self.get_next_instance(service_type)
        
        if not instance:
            raise Exception(f"No available instances for service {service_type}")

        try:
            await self.start_connection(service_type, instance['id'])
            result = await request_func(instance, *args, **kwargs)
            
            duration = time.time() - start_time
            LATENCY.labels(service_type=service_type).observe(duration)
            
            return result
            
        finally:
            await self.end_connection(service_type, instance['id'])

    async def get_load_metrics(self, service_type: str) -> Dict:
        """Get current load metrics for a service type"""
        instances = await service_registry.discover_service(service_type)
        metrics = {
            'service_type': service_type,
            'total_instances': len(instances),
            'total_connections': 0,
            'instance_loads': []
        }

        for instance in instances:
            instance_id = instance['id']
            connections = self.active_connections[f"{service_type}:{instance_id}"]
            metrics['total_connections'] += connections
            metrics['instance_loads'].append({
                'instance_id': instance_id,
                'connections': connections,
                'weight': self.service_weights[service_type].get(instance_id, 1.0)
            })

        return metrics

# Initialize global load balancer
load_balancer = LoadBalancer()