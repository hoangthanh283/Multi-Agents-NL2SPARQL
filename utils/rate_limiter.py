import logging
import time
from functools import wraps
from typing import Callable, Optional

import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

CIRCUIT_BREAKER_FAILURES = Counter(
    'circuit_breaker_failures_total',
    'Number of circuit breaker failures',
    ['service']
)

class RateLimiter:
    def __init__(self, redis_client: redis.Redis, key_prefix: str = "rate_limit:",
                 max_requests: int = 100, time_window: int = 60):
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.max_requests = max_requests
        self.time_window = time_window

    def is_allowed(self, key: str) -> bool:
        current_key = f"{self.key_prefix}{key}"
        current_time = int(time.time())
        window_key = f"{current_key}:{current_time // self.time_window}"

        try:
            requests = self.redis.incr(window_key)
            if requests == 1:
                self.redis.expire(window_key, self.time_window)
            return requests <= self.max_requests
        except redis.RedisError as e:
            logger.error(f"Redis error in rate limiter: {e}")
            return True  # Fail open in case of Redis issues

class CircuitBreaker:
    def __init__(self, redis_client: redis.Redis, service_name: str,
                 failure_threshold: int = 5, reset_timeout: int = 60):
        self.redis = redis_client
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.key = f"circuit_breaker:{service_name}"

    def is_open(self) -> bool:
        try:
            failures = int(self.redis.get(self.key) or 0)
            return failures >= self.failure_threshold
        except redis.RedisError as e:
            logger.error(f"Redis error in circuit breaker: {e}")
            return False

    def record_failure(self):
        try:
            failures = self.redis.incr(self.key)
            if failures == 1:
                self.redis.expire(self.key, self.reset_timeout)
            CIRCUIT_BREAKER_FAILURES.labels(service=self.service_name).inc()
        except redis.RedisError as e:
            logger.error(f"Redis error recording failure: {e}")

    def record_success(self):
        try:
            self.redis.delete(self.key)
        except redis.RedisError as e:
            logger.error(f"Redis error recording success: {e}")

def rate_limit(redis_client: redis.Redis, key_func: Callable,
               max_requests: int = 100, time_window: int = 60):
    limiter = RateLimiter(redis_client, max_requests=max_requests,
                         time_window=time_window)

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = key_func(*args, **kwargs)
            if not limiter.is_allowed(key):
                raise Exception("Rate limit exceeded")
            return await func(*args, **kwargs)
        return wrapper
    return decorator

def circuit_break(redis_client: redis.Redis, service_name: str,
                 failure_threshold: int = 5, reset_timeout: int = 60):
    breaker = CircuitBreaker(redis_client, service_name,
                           failure_threshold=failure_threshold,
                           reset_timeout=reset_timeout)

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if breaker.is_open():
                raise Exception(f"Circuit breaker open for {service_name}")
            try:
                result = await func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
        return wrapper
    return decorator