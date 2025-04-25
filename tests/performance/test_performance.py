import asyncio
import statistics
import time
from typing import Dict, List

import aiohttp
import numpy as np
import pytest
import ray
from prometheus_client import Histogram

from agents.langchian_master_agent import DistributedMasterAgent
from config.ray_config import init_ray_cluster

# Performance metrics
QUERY_LATENCY = Histogram('query_processing_latency_seconds',
                         'Query processing latency in seconds',
                         ['query_type'])

@pytest.fixture(scope="session")
async def ray_cluster():
    """Initialize Ray cluster for testing"""
    init_ray_cluster({
        "address": "local",
        "namespace": "perf_test",
        "include_dashboard": False
    })
    yield
    ray.shutdown()

@pytest.fixture(scope="session")
async def master_agent(ray_cluster):
    """Create master agent for testing"""
    agent = DistributedMasterAgent.remote("perf_test_master")
    yield agent
    await agent.shutdown.remote()

class PerformanceTest:
    def __init__(self):
        self.results = []

    def record_latency(self, latency: float):
        """Record a latency measurement"""
        self.results.append(latency)

    def get_statistics(self) -> Dict[str, float]:
        """Calculate performance statistics"""
        if not self.results:
            return {}

        return {
            'min': min(self.results),
            'max': max(self.results),
            'mean': statistics.mean(self.results),
            'median': statistics.median(self.results),
            'p95': np.percentile(self.results, 95),
            'p99': np.percentile(self.results, 99),
            'std_dev': statistics.stdev(self.results) if len(self.results) > 1 else 0
        }

@pytest.mark.asyncio
async def test_query_throughput(master_agent):
    """Test query processing throughput"""
    perf_test = PerformanceTest()
    num_queries = 100
    test_queries = [
        {
            "id": f"perf_test_{i}",
            "query": "What are the symptoms of COVID-19?",
            "timestamp": "2025-04-25T10:00:00Z"
        }
        for i in range(num_queries)
    ]

    async def process_query(query):
        start_time = time.time()
        result = await master_agent.process_task.remote(query)
        result = await ray.get(result)
        latency = time.time() - start_time
        
        perf_test.record_latency(latency)
        QUERY_LATENCY.labels(query_type='covid_symptoms').observe(latency)
        
        return result

    # Process queries concurrently
    tasks = [process_query(query) for query in test_queries]
    results = await asyncio.gather(*tasks)

    # Analyze results
    stats = perf_test.get_statistics()
    
    # Assertions for performance requirements
    assert stats['p95'] < 2.0  # 95th percentile should be under 2 seconds
    assert stats['median'] < 1.0  # median should be under 1 second
    assert all(r['success'] for r in results)  # all queries should succeed

@pytest.mark.asyncio
async def test_concurrent_users(master_agent):
    """Test system performance with concurrent users"""
    perf_test = PerformanceTest()
    num_users = 50
    requests_per_user = 10

    async def simulate_user(user_id: int):
        results = []
        for i in range(requests_per_user):
            start_time = time.time()
            query = {
                "id": f"user_{user_id}_query_{i}",
                "query": "List all infectious diseases",
                "timestamp": "2025-04-25T10:00:00Z"
            }
            
            result = await master_agent.process_task.remote(query)
            result = await ray.get(result)
            latency = time.time() - start_time
            
            perf_test.record_latency(latency)
            QUERY_LATENCY.labels(query_type='disease_list').observe(latency)
            
            results.append(result)
            await asyncio.sleep(0.1)  # Simulate think time
            
        return results

    # Simulate concurrent users
    user_tasks = [simulate_user(i) for i in range(num_users)]
    all_results = await asyncio.gather(*user_tasks)

    # Analyze results
    stats = perf_test.get_statistics()
    
    # Performance assertions
    assert stats['p99'] < 5.0  # 99th percentile should be under 5 seconds
    assert stats['mean'] < 2.0  # mean should be under 2 seconds
    assert all(all(r['success'] for r in user_results) 
              for user_results in all_results)

@pytest.mark.asyncio
async def test_system_scalability(master_agent):
    """Test system scalability under increasing load"""
    perf_test = PerformanceTest()
    load_levels = [10, 20, 50, 100]  # Number of concurrent requests

    async def generate_load(num_requests: int):
        tasks = []
        for i in range(num_requests):
            start_time = time.time()
            query = {
                "id": f"scale_test_{num_requests}_{i}",
                "query": "What are the risk factors for heart disease?",
                "timestamp": "2025-04-25T10:00:00Z"
            }
            
            task = master_agent.process_task.remote(query)
            tasks.append((task, start_time))

        results = []
        for task, start_time in tasks:
            result = await ray.get(task)
            latency = time.time() - start_time
            perf_test.record_latency(latency)
            QUERY_LATENCY.labels(query_type='risk_factors').observe(latency)
            results.append(result)

        return results

    # Test different load levels
    for load in load_levels:
        perf_test.results.clear()
        results = await generate_load(load)
        stats = perf_test.get_statistics()

        # Scalability assertions
        assert stats['p95'] < 3.0  # Should handle load within reasonable time
        assert all(r['success'] for r in results)
        
        # Log scalability metrics
        print(f"\nLoad level: {load} concurrent requests")
        print(f"Mean latency: {stats['mean']:.2f}s")
        print(f"95th percentile: {stats['p95']:.2f}s")
        print(f"Success rate: {sum(r['success'] for r in results) / len(results) * 100:.1f}%")

@pytest.mark.asyncio
async def test_long_running_stability():
    """Test system stability over a longer period"""
    test_duration = 300  # 5 minutes
    request_interval = 0.5  # Request every 0.5 seconds
    perf_test = PerformanceTest()

    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        while time.time() - start_time < test_duration:
            req_start = time.time()
            
            try:
                async with session.get("http://localhost:8000/health") as response:
                    latency = time.time() - req_start
                    perf_test.record_latency(latency)
                    assert response.status == 200
            except Exception as e:
                print(f"Request failed: {e}")
            
            await asyncio.sleep(request_interval)

    stats = perf_test.get_statistics()
    
    # Stability assertions
    assert stats['p99'] < 1.0  # 99th percentile should remain stable
    assert stats['std_dev'] < 0.5  # Low variance in response times

if __name__ == "__main__":
    pytest.main(["-v", "test_performance.py"])