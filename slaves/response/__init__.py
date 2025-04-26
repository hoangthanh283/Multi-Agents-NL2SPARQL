"""
Response domain slaves.

This package contains slave implementations for the Response domain:
- query_execution_slave: For executing SPARQL queries against endpoints
- response_generation_slave: For generating natural language responses from query results
"""

from slaves.response.query_execution_slave import QueryExecutionSlave
from slaves.response.response_generation_slave import ResponseGenerationSlave

__all__ = [
    "QueryExecutionSlave",
    "ResponseGenerationSlave"
]