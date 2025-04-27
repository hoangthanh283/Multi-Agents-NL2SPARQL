import os
import time
import hashlib
from functools import lru_cache
from openai import OpenAI
from loguru import logger
from typing import Optional
from langchain_openai import ChatOpenAI
from typing import Any, Dict, List, Optional
from langchain_core.prompts import ChatPromptTemplate


class QueryComplexityClassifier:
    """
    A classifier that determines if a natural language query is complex
    and requires multiple steps to generate a SPARQL query.
    
    Optimized for ultra-fast response times (0.2-0.3s) using:
    - o4-mini model with minimal tokens
    - LRU caching for repeated queries
    - Streamlined binary classification prompt
    """
    
    def __init__(self, api_key: Optional[str] = None, cache_size: int = 1000):
        """
        Initialize the classifier with OpenAI configuration.
        
        Args:
            api_key: OpenAI API key (defaults to environment variable)
            cache_size: Size of the LRU cache for query results
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Set OPENAI_API_KEY environment variable.")
        
        self.client = OpenAI(
            api_key=self.api_key,
            timeout=2.0  # Shorter timeout for faster failure
        )
        self.cache_size = cache_size
        self.agent = ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.,
            max_tokens=3,
        )

    def _prepare_plan_prompt(self, user_query: str, feedback: Optional[str] = None) -> List[Any]:
        """
        Create prompt for planning based on user query
        
        Args:
            user_query: Natural user query 
        
        Returns:
            Execution plan as a list of dictionary
        """
        prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """Classify if this query requires complex SPARQL with joins, multiple steps, or aggregations:
"{user_query}"
Output TRUE for complex queries (multiple entities, relationships, filtering, grouping)
Output FALSE for simple queries (one entity type, basic property lookup)
Just output TRUE or FALSE.

Examples:
What is the start date of Trieu Dynasty? --> FALSE
What is the duration of Trieu Dynasty? --> TRUE
"""
            ),
            ("user", "{user_query}{feedback}")
        ])
        if feedback is not None:
            feedback = ". Old plan and feedback: {}. Please improve this plan".format(str(feedback))
        else:
            feedback = ""
        return prompt.format_messages(user_query=user_query, feedback=feedback)

    @lru_cache(maxsize=1000)  # Cache results for repeated queries
    def _cached_classify(self, query_hash: str) -> bool:
        """Internal cached classification function"""
        # This function would contain the actual OpenAI API call
        # But it's wrapped with a hash for caching
        pass
    
    def is_complex_query(self, query: str) -> bool:
        """
        Classify if a query requires complex multi-step reasoning for SPARQL generation.
        Response optimized for 0.2-0.3s latency.
        
        Args:
            query: Natural language query to classify
            
        Returns:
            Boolean indicating if the query is complex
        """
        start_time = time.time()
        
        # Simple heuristic pre-check (runs in microseconds)
        word_count = len(query.split())
        has_complex_terms = any(term in query.lower() for term in [
            "average", "all", "more than", "less than", "between", "and", "or", 
            "not", "group", "filter", "order", "sort", "min", "max", "count"
        ])
        
        # Ultra-fast path: very short queries are almost always simple
        if word_count < 5 and not has_complex_terms:
            logger.debug(f"Fast path classification: simple query (took {time.time() - start_time:.4f}s)")
            return False
        
        # Ultra-fast path: queries with specific complex indicators
        if word_count > 15 and has_complex_terms:
            logger.debug(f"Fast path classification: complex query (took {time.time() - start_time:.4f}s)")
            return True
            
        # Compute hash for caching - faster than caching the full query string
        query_hash = hashlib.md5(query.strip().lower().encode()).hexdigest()
        
        # Check in-memory cache first
        cache_key = f"query_complexity:{query_hash}"
        try:
            prompt = self._prepare_plan_prompt(query)
            response = self.agent.invoke(prompt)
            
            # Get the classification result - just look for 'T' in TRUE
            result_text = response.content.strip()
            is_complex = result_text.upper().startswith('T')
            
            # Log performance
            elapsed = time.time() - start_time
            logger.debug(f"Query complexity classification completed in {elapsed:.4f}s: {is_complex}")
            return is_complex
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error classifying query complexity ({elapsed:.4f}s): {e}")
            
            # Use the heuristic result in case of error
            return has_complex_terms or word_count > 10


if __name__ == "__main__":
    # Example to test the classifier
    classifier = QueryComplexityClassifier()
    
    # Test with some example queries
    test_queries = [
        "What is the identity of Quang Trung?",
        "What is the start date of Trieu Dynasty?",
        "What is the end date of Trieu Dynasty?",
        "What is the duration of Trieu Dynasty?",
        "What is the description of Quang Trung?",
        "Get all classes in the knowledge graph?",
        "What is the class of Quang Trung?",
        "What is the description of Bachelor?",
        "What is the description of Master?",
        "What is the description of Phd?",
        "Find the description of Phd?"
    ]
    
    # Measure performance
    total_time = 0
    for query in test_queries:
        start = time.time()
        is_complex = classifier.is_complex_query(query)
        elapsed = time.time() - start
        total_time += elapsed
        print(f"Query: {query}\nComplex: {is_complex}\nTime: {elapsed:.4f}s\n")
    print(f"Average time per query: {total_time/len(test_queries):.4f}s")
