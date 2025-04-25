import asyncio
import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, Optional

from prometheus_client import Counter, Gauge

from config.logging_config import get_logger

logger = get_logger(__name__, 'resilience')

# Metrics
CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state (0=open, 1=closed)', ['service'])
CIRCUIT_BREAKER_FAILURES = Counter('circuit_breaker_failures_total', 'Number of circuit breaker failures', ['service'])
RETRY_ATTEMPTS = Counter('retry_attempts_total', 'Number of retry attempts', ['operation'])

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    def __init__(self, name: str, failure_threshold: int = 5, reset_timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, or half-open

    def record_failure(self):
        """Record a failure and potentially open the circuit"""
        self.failures += 1
        self.last_failure_time = datetime.now()
        CIRCUIT_BREAKER_FAILURES.labels(service=self.name).inc()

        if self.failures >= self.failure_threshold:
            self.state = "open"
            CIRCUIT_BREAKER_STATE.labels(service=self.name).set(0)
            logger.warning(f"Circuit breaker opened for {self.name}")

    def record_success(self):
        """Record a success and potentially close the circuit"""
        self.failures = 0
        if self.state == "half-open":
            self.state = "closed"
            CIRCUIT_BREAKER_STATE.labels(service=self.name).set(1)
            logger.info(f"Circuit breaker closed for {self.name}")

    def can_execute(self) -> bool:
        """Check if the operation can be executed"""
        if self.state == "closed":
            return True
        elif self.state == "open":
            # Check if enough time has passed to try again
            if self.last_failure_time and \
               datetime.now() - self.last_failure_time > timedelta(seconds=self.reset_timeout):
                self.state = "half-open"
                return True
            return False
        else:  # half-open
            return True

class RetryWithBackoff:
    """Retry pattern with exponential backoff"""
    def __init__(self, max_retries: int = 3, initial_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay

    async def execute(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute an operation with retry logic"""
        last_exception = None
        delay = self.initial_delay

        for attempt in range(self.max_retries + 1):
            try:
                RETRY_ATTEMPTS.labels(operation=operation.__name__).inc()
                return await operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt == self.max_retries:
                    break

                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.max_delay)

        raise last_exception

def with_circuit_breaker(circuit_breaker: CircuitBreaker):
    """Decorator to apply circuit breaker pattern"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not circuit_breaker.can_execute():
                raise Exception(f"Circuit breaker is open for {circuit_breaker.name}")

            try:
                result = await func(*args, **kwargs)
                circuit_breaker.record_success()
                return result
            except Exception as e:
                circuit_breaker.record_failure()
                raise

        return wrapper
    return decorator

def with_retry(retry_handler: RetryWithBackoff):
    """Decorator to apply retry pattern"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            return await retry_handler.execute(func, *args, **kwargs)
        return wrapper
    return decorator

class RateLimiter:
    """Token bucket rate limiter"""
    def __init__(self, rate: float, burst: int):
        self.rate = rate  # tokens per second
        self.burst = burst  # maximum tokens
        self.tokens = burst
        self.last_update = time.time()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens from the bucket"""
        now = time.time()
        time_passed = now - self.last_update
        self.tokens = min(self.burst, self.tokens + time_passed * self.rate)
        self.last_update = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

def with_rate_limit(rate_limiter: RateLimiter, tokens: int = 1):
    """Decorator to apply rate limiting"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not await rate_limiter.acquire(tokens):
                raise Exception("Rate limit exceeded")
            return await func(*args, **kwargs)
        return wrapper
    return decorator

# Example usage:
# circuit_breaker = CircuitBreaker("graphdb_service")
# retry_handler = RetryWithBackoff()
# rate_limiter = RateLimiter(10.0, 100)  # 10 requests per second, burst of 100

# @with_circuit_breaker(circuit_breaker)
# @with_retry(retry_handler)
# @with_rate_limit(rate_limiter)
# async def make_request():
#     # Make the actual request here
#     pass