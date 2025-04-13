import os
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient as BaseQdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import FieldCondition, Filter, MatchValue

from models.embeddings import EmbeddingModel


class QdrantClient:
    """
    Client for Qdrant vector database operations.
    Handles vector search for tool selection, example retrieval, etc.
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
                    distance=models.Distance.COSINE
                )
            )
            return True
        except Exception as e:
            print(f"Error creating collection: {e}")
            return False
    
    def upsert_points(
        self, 
        collection_name: str, 
        points: List[Dict[str, Any]], 
        embedding_model: EmbeddingModel
    ) -> bool:
        """
        Insert or update points in a collection.
        
        Args:
            collection_name: Name of the collection
            points: List of points with IDs and payloads
            embedding_model: Model to generate embeddings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare point data
            point_data = []
            for point in points:
                point_id = point.get("id")
                payload = point.get("payload", {})
                text = point.get("text", "")
                
                # Generate embedding if text is provided
                if text:
                    vector = embedding_model.embed(text)
                else:
                    # Skip if no text to embed
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
            self.client.upsert(
                collection_name=collection_name,
                points=point_data
            )
            return True
        except Exception as e:
            print(f"Error upserting points: {e}")
            return False
    
    def search_tools(
        self, 
        collection_name: str, 
        query_text: str, 
        embedding_model: EmbeddingModel, 
        threshold: float = 0.7, 
        limit: int = 5, 
        filter_by: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """
        Search for relevant tools using vector similarity.
        
        Args:
            collection_name: Name of the collection
            query_text: Text query to search for
            embedding_model: Model to generate embedding
            threshold: Similarity threshold
            limit: Maximum number of results
            filter_by: Optional filter conditions
            
        Returns:
            List of search results
        """
        try:
            # Generate embedding for the query
            query_vector = embedding_model.embed(query_text)
            
            # Prepare filter if provided
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
                
            # Search the collection
            search_results = self.client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                limit=limit,
                score_threshold=threshold,
                filter=search_filter
            )
            
            return search_results
        except Exception as e:
            print(f"Error searching tools: {e}")
            return []
    
    def search_examples(
        self, 
        collection_name: str, 
        query_text: str, 
        limit: int = 3
    ) -> List[Any]:
        """
        Search for similar examples using vector similarity.
        This is a simplified version that assumes embeddings are pre-computed.
        
        Args:
            collection_name: Name of the collection
            query_text: Text query to search for
            limit: Maximum number of results
            
        Returns:
            List of search results
        """
        try:
            # For simplicity, we assume there's a default embedding model
            # In a real implementation, you would initialize the model properly
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('all-MiniLM-L6-v2')
            query_vector = model.encode(query_text).tolist()
            
            # Search the collection
            search_results = self.client.search(
                collection_name=collection_name,
                query_vector=query_vector,
                limit=limit
            )
            
            return search_results
        except Exception as e:
            print(f"Error searching examples: {e}")
            return []
    
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
                points_selector=point_ids
            )
            return True
        except Exception as e:
            print(f"Error deleting points: {e}")
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
            print(f"Error checking collection: {e}")
            return False
