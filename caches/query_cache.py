from elasticsearch import Elasticsearch
from redis import Redis
import json
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from loguru import logger
import time

class QueryCache:
    def __init__(self, redis_host: str, redis_port: str, redis_ttl: int):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_ttl = redis_ttl
        
    def search(self, query, query_prefix):
        pass
    
    def save(self, query, query_prefix, response):
        pass
    
class SPARQLQueryCache(QueryCache):
    def __init__(self, redis_host: str, redis_port: str, redis_ttl: int):
        super().__init__(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_ttl=redis_ttl
        )
        
        self.redis_client = Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True
        )
    
    def search(self, query, query_prefix):
        key = self._get_key(query=query, query_prefix=query_prefix)
        redis_result = self.redis_client.get(key)
        if redis_result:
            logger.info("Redis cache hit")
            result = json.loads(redis_result)
            if "result" not in result:
                logger.info("Redis cache miss")
                return None
            else:
                return result["result"]
        else:    
            logger.info("Redis cache miss")
            
    def save(self, query, query_prefix, response): 
        key = self._get_key(query=query, query_prefix=query_prefix)
        self.redis_client.setex(
            key,
            self.redis_ttl,
            json.dumps({
                "result": response,
                "timestamp": time.time()
            })
        )
        logger.info("Cache query successfully")
        
    def _get_key(self, query, query_prefix=None):
        query_hash = hashlib.md5(query.lower().encode()).hexdigest()
        query_hash = query_hash if not(query_prefix) else query_prefix + str(query_hash)
        return query_hash
    
class ConstructionQueryCache(QueryCache):
    def __init__(self, redis_host: str, redis_port: int, 
                 es_host: str, es_port: int, es_index: str,
                 similarity_threshold: float, redis_ttl: int):
        super().__init__(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_ttl=redis_ttl
        )
        
        self.redis_client = Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True
        )
        
        self.es_client = Elasticsearch([
            f"http://{es_host}:{es_port}"
        ])
        self.es_index = es_index
        self.similarity_threshold = similarity_threshold
        
        self._create_es_index()
        
    def search(self, query, query_prefix):
        key = self._get_key(query=query, query_prefix=query_prefix)
        redis_result = self._search_redis(key)
        if redis_result:
            logger.info("Redis cache hit")
            return redis_result
        else:
            es_result = self._search_elasticsearch(query)
            if es_result:
                logger.info("ES hit")
                return es_result
        logger.info("Cache miss")
        return None
        
    def save(self, query, query_prefix, response): 
        key = self._get_key(query=query, query_prefix=query_prefix)
        self.redis_client.setex(
            key,
            self.redis_ttl,
            json.dumps({
                "sparql": response["sparql"],
                "query_type": response["query_type"]
            })
        ) 
        self.es_client.index(
            index=self.es_index,
            body={
                "query": query.lower(), 
                "response": response["sparql"], 
                "query_hash": key,
                "query_type": response["query_type"],
                "timestamp": int(time.time() * 1000)
            }
        )
        
    def _search_redis(self, key):
        redis_result = self.redis_client.get(key)
        if redis_result:
            result = json.loads(redis_result)
            return {
                "sparql": result["sparql"],
                "query_type": result["query_type"],
                "template_based": False,
                "llm_generated": False,
                "entities_used": []
            }
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
            "_source": ["query", "response", "query_hash", "query_type"],
            "size": 1
        }
        
        try:
            result = self.es_client.search(index=self.es_index, body=search_body)
            hits = result.get('hits', {}).get('hits', [])
            
            if hits and hits[0]['_score'] > self.similarity_threshold:
                source = hits[0]['_source']
                return {
                    "sparql": source["response"],
                    "query_type": result["query_type"],
                    "template_based": False,
                    "llm_generated": False,
                    "entities_used": []
                }
        except Exception as e:
            logger.error(f"Error searching Elasticsearch: {e}")
        
        return None
        
    def _get_key(self, query, query_prefix=None):
        query_hash = hashlib.md5(query.lower().encode()).hexdigest()
        query_hash = query_hash if not(query_prefix) else query_prefix + str(query_hash)
        return query_hash
        
    def _create_es_index(self):
        """
        Create elastic search index with BM25 settings if it does not exist
        """
        if not self.es_client.indices.exists(index=self.es_index):
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
                        "query_type": {
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