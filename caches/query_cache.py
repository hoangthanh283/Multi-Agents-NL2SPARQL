from elasticsearch import Elasticsearch
from redis import Redis
import json
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from loguru import logger
import time

class QueryCache:
    def __init__(self, redis_host: str, redis_port: int, 
                 es_host: str, es_port: int, es_index: str,
                 similarity_threshold: float, redis_ttl: int):
        
        self.redis_client = Redis(
            host=redis_host, 
            port=redis_port,
            decode=responses=True
        )
        self.redis_ttl = redis_ttl
        
        self.es_client = Elasticsearch([
            f"http://{es_host}:{es_port}"
        ])
        self.es_index = es_index
        self.similarity_threshold = similarity_threshold
        
        self._create_es_index()
        
    def _create_es_index(self):
        """
        Create elastic search index with BM25 settings if it does not exist
        """
        if not self.es_client.indices.exists(index=self.es_index):
            # Define index settings with BM25 similarity
            settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "similarity": {
                        "default": {
                            "type": "BM25",
                            "k1": 1.2,
                            "b": 0.75
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "query": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "query_hash": {
                            "type": "keyword"
                        },
                        "response": {
                            "type": "text" 
                        },
                        "timestamp": {
                            "type": "date"
                        }
                    }
                }
            }
            
            self.es_client.indices.create(index=self.es_index, body=settings)
            logger.info(f"Created Elasticsearch index '{self.es_index}'")
        
    def _generate_query_hash(self, query: str) -> str:
        """Generate a unique hash for the query."""
        return hashlib.md5(query.encode()).hexdigest() 
    
    def get_response(self, query: str) -> Optional[str]:
        query_hash = self._generate_query_hash(query)
        
        # Check redis
        redis_result = self.redis_client.get(query_hash)
        if redis_result:
            logger.info("Exact match found in Redis cache")
            return json.loads(redis_result)["response"]
        
        es_result = self._search_elasticsearch(query)
        if es_result:
            logger.info(f"Similar query found in Elastic Search: {es_result['query']}")
            self._store_in_redis(query_hash, es_result)
            return es_result['response']
        
        return None
        
    def _search_elasticsearch(self, query: str) -> Optional[Dict[str, Any]]:
        """Search Elasticsearch for similar queries using BM25."""
        search_body = {
            "query": {
                "match": {
                    "query": {
                        "query": query,
                        "operator": "and",
                        "minimum_should_match": "70%"  # Adjust as needed
                    }
                }
            },
            "_source": ["query", "response", "query_hash"],
            "size": 1
        }
        
        try:
            result = self.es_client.search(index=self.es_index, body=search_body)
            hits = result.get('hits', {}).get('hits', [])
            
            if hits and hits[0]['_score'] > self.similarity_threshold:
                source = hits[0]['_source']
                return {
                    'query': source['query'],
                    'response': source['response'],
                    'query_hash': source['query_hash']
                }
        except Exception as e:
            logger.error(f"Error searching Elasticsearch: {e}")
        
        return None
    
    def store_response(self, query: str, response: str) -> None:
        """Store a query and its response in both Redis and Elasticsearch."""
        query_hash = self._generate_query_hash(query)
        
        # Store in Redis
        self._store_in_redis(query_hash, {
            'query': query,
            'response': response,
            'query_hash': query_hash
        })
        
        # Store in Elasticsearch
        self._store_in_elasticsearch(query, response, query_hash)
        
        logger.info(f"Stored new query-response pair in cache: {query[:30]}...")
    
    def _store_in_redis(self, query_hash: str, data: Dict[str, Any]) -> None:
        """Store data in Redis with TTL."""
        self.redis_client.setex(
            query_hash,
            self.redis_ttl,
            json.dumps({
                'query': data['query'],
                'response': data['response'],
                'query_hash': data['query_hash']
            })
        )
    
    def _store_in_elasticsearch(self, query: str, response: str, query_hash: str) -> None:
        """Store query and response in Elasticsearch."""
        document = {
            'query': query,
            'response': response,
            'query_hash': query_hash,
            'timestamp': int(time.time() * 1000)  # Current time in milliseconds
        }
        
        try:
            self.es_client.index(index=self.es_index, body=document)
        except Exception as e:
            logger.error(f"Error storing in Elasticsearch: {e}")