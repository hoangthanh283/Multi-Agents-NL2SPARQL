# database/qdrant_client.py
import logging
import os
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient as BaseQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import (Distance, FieldCondition, Filter,
                                       MatchValue)
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

class QdrantClient:
    """
    Client for Qdrant vector database operations.
    Handles vector search for SPARQL templates, examples, etc.
    """
    
    def __init__(self, url: Optional[str] = None, api_key: Optional[str] = None):
        """
        Initialize the Qdrant client.
        
        Args:
            url: URL of the Qdrant server, defaults to env var or localhost
            api_key: API key for authentication, defaults to env var
        """
        self.url = url or os.getenv("QDRANT_URL", "http://localhost:6333")
        self.api_key = api_key or os.getenv("QDRANT_API_KEY")
        self.default_model = SentenceTransformer("all-MiniLM-L6-v2")
        
        # Initialize the base client
        self.client = BaseQdrantClient(
            url=self.url,
            api_key=self.api_key
        )
        
        # Default vector dimension for embedding models
        self.default_dim = 768
    
    def create_collection(
        self, 
        collection_name: str, 
        vector_dim: int = None
    ) -> bool:
        """
        Create a new collection in Qdrant.
        
        Args:
            collection_name: Name of the collection
            vector_dim: Dimension of the vectors
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use default dimension if not specified
            if vector_dim is None:
                vector_dim = self.default_dim
                
            # Create the collection
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=vector_dim,
                    distance=Distance.COSINE
                )
            )
            return True
        except Exception as e:
            logger.error(f"Error creating collection: {e}")
            return False
    
    def search(
        self, 
        collection_name: str, 
        query_text: str, 
        embedding_model=None, 
        limit: int = 5,
        threshold: float = 0.7,
        filter_by: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Search for similar vectors in the collection using query_points API.
        
        Args:
            collection_name: Name of the collection
            query_text: Text query to search for
            embedding_model: Model to generate embedding
            limit: Maximum number of results
            threshold: Similarity threshold
            filter_by: Optional filter conditions
            
        Returns:
            List of search results
        """
        try:
            # Generate embedding for the query
            if embedding_model:
                query_vector = embedding_model.embed(query_text)
            else:
                # Fallback to a default embedding if no model provided.
                query_vector = self.default_model.encode(query_text).tolist()
            
            # Prepare search filter if provided
            search_filter = None
            if filter_by:
                filter_conditions = []
                for field, value in filter_by.items():
                    filter_conditions.append(
                        FieldCondition(
                            key=field,
                            match=MatchValue(value=value)
                        )
                    )
                search_filter = Filter(
                    must=filter_conditions
                )
            
            # Use query_points method from Qdrant API
            search_results = self.client.query_points(
                collection_name=collection_name,
                query_vector=query_vector,
                query_filter=search_filter,
                limit=limit,
                score_threshold=threshold
            )
            
            # Create a simplified result format that mimics the previous implementation
            simplified_results = []
            for point in search_results.points:
                simplified_results.append({
                    "id": point.id,
                    "payload": point.payload,
                    "score": point.score
                })
            return simplified_results
        except Exception as e:
            logger.error(f"Error searching in collection: {e}")
            return []
    
    def upsert_points(
        self, 
        collection_name: str, 
        points: List[Dict[str, Any]]
    ) -> bool:
        """
        Insert or update points in a collection.
        
        Args:
            collection_name: Name of the collection
            points: List of points with IDs, vectors, and payloads
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare point data
            point_data = []
            for point in points:
                point_id = point.get("id")
                payload = point.get("payload", {})
                vector = point.get("vector", [])
                
                # Skip if no vector
                if not vector:
                    logger.warning(f"Skipping point {point_id} - no vector provided")
                    continue
                    
                # Add the point to batch
                point_data.append(
                    models.PointStruct(
                        id=point_id,
                        vector=vector,
                        payload=payload
                    )
                )
                
            # Upsert the points in batch
            if point_data:
                self.client.upsert(
                    collection_name=collection_name,
                    points=point_data
                )
                return True
            else:
                logger.warning("No valid points to upsert")
                return False
        except Exception as e:
            logger.error(f"Error upserting points: {e}")
            return False
    
    def delete_points(self, collection_name: str, point_ids: List[str]) -> bool:
        """
        Delete points from a collection.
        
        Args:
            collection_name: Name of the collection
            point_ids: List of point IDs to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.delete(
                collection_name=collection_name,
                points_selector=models.PointIdsList(
                    points=point_ids
                )
            )
            return True
        except Exception as e:
            logger.error(f"Error deleting points: {e}")
            return False
    
    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            True if exists, False otherwise
        """
        try:
            collections = self.client.get_collections()
            for collection in collections.collections:
                if collection.name == collection_name:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking collection: {e}")
            return False
