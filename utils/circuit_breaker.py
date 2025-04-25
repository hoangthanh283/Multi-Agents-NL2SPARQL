import asyncio
import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, Optional

from prometheus_client import Counter, Gauge, Histogram

from config.logging_config import get_logger

logger = get_logger(__name__, 'circuit_breaker')

# Metrics
CIRCUIT_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state (0=open, 1=half-open, 2=closed)', ['service'])
FAILURE_COUNT = Counter('circuit_breaker_failures_total', 'Number of circuit breaker failures', ['service'])
RECOVERY_TIME = Histogram('circuit_breaker_recovery_time_seconds', 'Time taken to recover from failure', ['service'])

class CircuitState(Enum):
    OPEN = 0      # Circuit is open (failing fast)
    HALF_OPEN = 1 # Testing if service has recovered
    CLOSED = 2    # Circuit is closed (normal operation)

class CircuitBreaker:
    def __init__(self, 
                 name: str,
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 half_open_timeout: int = 30,
                 error_types: tuple = (Exception,)):
        """
        Initialize circuit breaker
        
        Args:
            name: Service name for metrics and logging
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            half_open_timeout: Seconds to wait in half-open state
            error_types: Types of exceptions to count as failures
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_timeout = half_open_timeout
        self.error_types = error_types
        
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.last_failure_time = None
        self.last_state_change = datetime.now()
        
        # For half-open state
        self.test_calls = 0
        self.test_failures = 0
        
        # Update initial state metric
        CIRCUIT_STATE.labels(service=name).set(self.state.value)

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Make a call through the circuit breaker
        
        Args:
            func: Async function to call
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func
            
        Returns:
            Result from func if successful
            
        Raises:
            Exception if circuit is open or call fails
        """
        await self._check_state_transition()
        
        if self.state == CircuitState.OPEN:
            raise Exception(f"Circuit breaker for {self.name} is OPEN")
            
        try:
            if self.state == CircuitState.HALF_OPEN:
                self.test_calls += 1
            
            start_time = time.time()
            result = await func(*args, **kwargs)
            
            if self.state == CircuitState.HALF_OPEN:
                if self.test_calls >= 3 and self.test_failures == 0:
                    await self._transition_to_closed()
                    recovery_time = time.time() - start_time
                    RECOVERY_TIME.labels(service=self.name).observe(recovery_time)
                    
            return result
            
        except self.error_types as e:
            await self._handle_failure(e)
            raise

    async def _handle_failure(self, error: Exception):
        """Handle a failed call"""
        FAILURE_COUNT.labels(service=self.name).inc()
        
        if self.state == CircuitState.HALF_OPEN:
            self.test_failures += 1
            if self.test_failures >= 2:
                await self._transition_to_open()
        else:
            self.failures += 1
            self.last_failure_time = datetime.now()
            
            if self.failures >= self.failure_threshold:
                await self._transition_to_open()
                
        logger.error(f"Circuit breaker {self.name} failure: {str(error)}")

    async def _check_state_transition(self):
        """Check if state transition is needed based on timeouts"""
        now = datetime.now()
        
        if self.state == CircuitState.OPEN:
            if now - self.last_state_change > timedelta(seconds=self.recovery_timeout):
                await self._transition_to_half_open()
                
        elif self.state == CircuitState.HALF_OPEN:
            if now - self.last_state_change > timedelta(seconds=self.half_open_timeout):
                await self._transition_to_open()

    async def _transition_to_open(self):
        """Transition to OPEN state"""
        self.state = CircuitState.OPEN
        self.last_state_change = datetime.now()
        CIRCUIT_STATE.labels(service=self.name).set(self.state.value)
        logger.warning(f"Circuit breaker {self.name} transitioned to OPEN")

    async def _transition_to_half_open(self):
        """Transition to HALF_OPEN state"""
        self.state = CircuitState.HALF_OPEN
        self.last_state_change = datetime.now()
        self.test_calls = 0
        self.test_failures = 0
        CIRCUIT_STATE.labels(service=self.name).set(self.state.value)
        logger.info(f"Circuit breaker {self.name} transitioned to HALF_OPEN")

    async def _transition_to_closed(self):
        """Transition to CLOSED state"""
        self.state = CircuitState.CLOSED
        self.last_state_change = datetime.now()
        self.failures = 0
        self.last_failure_time = None
        CIRCUIT_STATE.labels(service=self.name).set(self.state.value)
        logger.info(f"Circuit breaker {self.name} transitioned to CLOSED")

    def get_state(self) -> Dict:
        """Get current circuit breaker state"""
        return {
            'name': self.name,
            'state': self.state.name,
            'failures': self.failures,
            'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'last_state_change': self.last_state_change.isoformat()
        }

def circuit_breaker(name: str, **cb_kwargs):
    """
    Decorator for adding circuit breaker to async functions
    
    Args:
        name: Service name for the circuit breaker
        **cb_kwargs: Arguments to pass to CircuitBreaker constructor
        
    Returns:
        Decorated function with circuit breaker protection
    """
    circuit = CircuitBreaker(name, **cb_kwargs)
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await circuit.call(func, *args, **kwargs)
        return wrapper
    return decorator

# Create circuit breakers for main services
graphdb_circuit = CircuitBreaker('graphdb', failure_threshold=3, recovery_timeout=30)
redis_circuit = CircuitBreaker('redis', failure_threshold=3, recovery_timeout=30)
ray_circuit = CircuitBreaker('ray', failure_threshold=3, recovery_timeout=30)