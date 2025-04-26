import asyncio
import json
import os
from typing import Any, Dict

import aiohttp
import pytest
import ray

from agents.langchain_master_agent import DistributedMasterAgent
from config.cluster_config import cluster_config
from config.ray_config import init_ray_cluster
from utils.backup_manager import backup_manager
from utils.health_checker import health_checker
from utils.kafka_handler import QUERY_TOPIC, RESULT_TOPIC, kafka_handler


@pytest.fixture(scope="session")
async def ray_cluster():
    """Initialize Ray cluster for testing"""
    init_ray_cluster({
        "address": "local",
        "namespace": "test",
        "include_dashboard": False
    })
    yield
    ray.shutdown()

@pytest.fixture(scope="session")
async def master_agent(ray_cluster):
    """Create master agent for testing"""
    agent = DistributedMasterAgent.remote("test_master")
    yield agent
    await agent.shutdown.remote()

@pytest.fixture(scope="session")
async def kafka_producer():
    """Initialize Kafka producer for testing"""
    yield kafka_handler
    await kafka_handler.stop()

@pytest.mark.asyncio
async def test_distributed_query_processing(master_agent, kafka_producer):
    """Test distributed natural language query processing"""
    # Test query
    test_query = {
        "id": "test_1",
        "query": "What are the main symptoms of COVID-19?",
        "timestamp": "2025-04-25T10:00:00Z"
    }
    
    # Process query through master agent
    result = await master_agent.process_task.remote(test_query)
    result = await ray.get(result)
    
    assert result["success"] is True
    assert "sparql" in result
    assert "entities" in result
    assert "mappings" in result

@pytest.mark.asyncio
async def test_service_health_monitoring():
    """Test health monitoring system"""
    # Start health monitoring
    monitor_task = asyncio.create_task(health_checker.start_monitoring())
    
    # Wait for initial health checks
    await asyncio.sleep(2)
    
    # Check specific service health
    async with aiohttp.ClientSession() as session:
        result = await health_checker.check_service_health(
            session,
            "graphdb",
            "http://localhost:7200/rest/health"
        )
        
        assert result["healthy"] is True
        assert "latency" in result
    
    # Stop monitoring
    await health_checker.stop_monitoring()
    await monitor_task

@pytest.mark.asyncio
async def test_backup_and_restore(tmp_path):
    """Test backup and restore functionality"""
    # Create test backup
    test_service = "redis"
    success = await backup_manager.create_backup(test_service)
    assert success is True
    
    # Find latest backup
    backups = os.listdir(backup_manager.backup_dir)
    latest_backup = sorted(
        [b for b in backups if b.startswith(f"{test_service}_")],
        reverse=True
    )[0]
    
    # Test restore
    backup_path = os.path.join(backup_manager.backup_dir, latest_backup)
    success = await backup_manager.restore_backup(test_service, backup_path)
    assert success is True

@pytest.mark.asyncio
async def test_kafka_event_streaming(kafka_producer):
    """Test Kafka event streaming"""
    test_message = {
        "id": "test_2",
        "query": "List all infectious diseases.",
        "timestamp": "2025-04-25T11:00:00Z"
    }
    
    # Produce message
    kafka_producer.produce_message(QUERY_TOPIC, test_message)
    
    # Setup consumer for testing
    received_messages = []
    
    async def message_handler(message):
        received_messages.append(message)
    
    # Start consuming
    consume_task = asyncio.create_task(
        kafka_producer.consume_messages([QUERY_TOPIC], message_handler)
    )
    
    # Wait for message processing
    await asyncio.sleep(2)
    
    # Verify message received
    assert len(received_messages) > 0
    assert received_messages[0]["id"] == test_message["id"]
    
    # Stop consuming
    kafka_producer.stop_consuming()
    await consume_task

@pytest.mark.asyncio
async def test_load_balancing():
    """Test load balancing and scaling"""
    # Get service configuration
    api_config = cluster_config.get_service_config("api")
    assert api_config is not None
    assert api_config.replicas > 1
    
    # Test multiple concurrent requests
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(10):
            task = session.get("http://localhost:8000/health")
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        
        # Verify all requests successful
        for response in responses:
            assert response.status == 200

@pytest.mark.asyncio
async def test_fault_tolerance(master_agent):
    """Test system fault tolerance"""
    # Simulate failed sub-agent
    await ray.kill(master_agent.sub_agents['entity_recognition'])
    
    # Test query should still work due to automatic restart
    test_query = {
        "id": "test_3",
        "query": "What are the treatments for diabetes?",
        "timestamp": "2025-04-25T12:00:00Z"
    }
    
    # Process should succeed despite failure
    result = await master_agent.process_task.remote(test_query)
    result = await ray.get(result)
    
    assert result["success"] is True

if __name__ == "__main__":
    pytest.main(["-v", "test_distributed_system.py"])