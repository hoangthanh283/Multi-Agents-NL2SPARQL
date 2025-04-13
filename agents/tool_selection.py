import json
from typing import Any, Dict, List, Optional

from database.qdrant_client import QdrantClient
from models.embeddings import BiEncoderModel, CrossEncoderModel
from tools.template_tools import TemplateTools


class ToolSelectionAgent:
    """
    Slave agent responsible for selecting appropriate SPARQL templates and
    query patterns based on the user query.
    """
    
    def __init__(
        self, 
        qdrant_client: QdrantClient, 
        embedding_model: BiEncoderModel,
        reranking_model: Optional[CrossEncoderModel] = None,
        template_tools: Optional[TemplateTools] = None
    ):
        """
        Initialize the tool selection agent.
        
        Args:
            qdrant_client: Client for the vector database
            embedding_model: Bi-encoder embedding model
            reranking_model: Optional cross-encoder reranking model
            template_tools: Template utilities
        """
        self.qdrant_client = qdrant_client
        self.embedding_model = embedding_model
        self.reranking_model = reranking_model
        self.template_tools = template_tools or TemplateTools()
        
        # Collection name for query patterns
        self.patterns_collection = "query_patterns"
        
        # Collection name for SPARQL examples
        self.examples_collection = "sparql_examples"
        
        # Threshold for initial vector search
        self.initial_search_threshold = 0.65
        
        # Threshold for reranking
        self.reranking_threshold = 0.5
        
        # Maximum number of templates to return
        self.max_templates = 5
    
    def select_templates(
        self, 
        query: str, 
        mapped_entities: Dict[str, List[Dict[str, Any]]],
        query_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Select appropriate SPARQL templates for a given query.
        
        Args:
            query: The refined user query
            mapped_entities: Dictionary of entities mapped to ontology terms
            query_type: Optional query type (SELECT, ASK, CONSTRUCT, DESCRIBE)
            
        Returns:
            List of selected templates with scores
        """
        # Step 1: Count entity types to filter templates
        entity_counts = {
            "classes": len(mapped_entities.get("classes", [])),
            "properties": len(mapped_entities.get("properties", [])),
            "instances": len(mapped_entities.get("instances", [])),
            "literals": len(mapped_entities.get("literals", []))
        }
        
        # Find templates that match the entity counts
        matching_templates = self.template_tools.find_templates_for_entities(entity_counts)
        
        if not matching_templates:
            return []
        
        # Step 2: Filter by query type if provided
        if query_type:
            matching_templates = [t for t in matching_templates if t.get("query_type", "").upper() == query_type.upper()]
            
            if not matching_templates:
                return []
        
        # Step 3: Search for similar query patterns using vector search
        similar_patterns = self._search_similar_patterns(query)
        
        # Step 4: Score templates based on similarity to query
        scored_templates = self._score_templates(query, matching_templates, similar_patterns)
        
        # Step 5: Rerank if reranking model is available
        if self.reranking_model and scored_templates:
            reranked_templates = self._rerank_templates(query, scored_templates)
            return reranked_templates[:self.max_templates]
        
        return scored_templates[:self.max_templates]
    
    def select_sparql_examples(
        self, 
        query: str, 
        mapped_entities: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        Select similar SPARQL query examples for a given query.
        
        Args:
            query: The refined user query
            mapped_entities: Dictionary of entities mapped to ontology terms
            
        Returns:
            List of similar SPARQL examples
        """
        try:
            # Search for similar examples in the vector database
            search_results = self.qdrant_client.search(
                collection_name=self.examples_collection,
                query_text=query,
                embedding_model=self.embedding_model,
                threshold=self.initial_search_threshold,
                limit=self.max_templates
            )
            
            # Format the results
            examples = []
            for result in search_results:
                examples.append({
                    "natural_language": result.payload.get("natural_language", ""),
                    "sparql": result.payload.get("sparql", ""),
                    "entities": result.payload.get("entities", {}),
                    "score": result.score
                })
                
            return examples
        except Exception as e:
            print(f"Error searching for SPARQL examples: {e}")
            return []
    
    def _search_similar_patterns(self, query: str) -> List[Dict[str, Any]]:
        """
        Search for similar query patterns using vector similarity.
        
        Args:
            query: The refined user query
            
        Returns:
            List of similar query patterns
        """
        try:
            # Search for similar patterns in the vector database
            search_results = self.qdrant_client.search(
                collection_name=self.patterns_collection,
                query_text=query,
                embedding_model=self.embedding_model,
                threshold=self.initial_search_threshold,
                limit=self.max_templates * 2  # Get more candidates for scoring
            )
            
            # Format the results
            patterns = []
            for result in search_results:
                patterns.append({
                    "pattern": result.payload.get("pattern", ""),
                    "template_ids": result.payload.get("template_ids", []),
                    "keywords": result.payload.get("keywords", []),
                    "score": result.score
                })
                
            return patterns
        except Exception as e:
            print(f"Error searching for query patterns: {e}")
            return []
    
    def _score_templates(
        self, 
        query: str, 
        templates: List[Dict[str, Any]], 
        patterns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Score templates based on similarity to query and patterns.
        
        Args:
            query: The refined user query
            templates: List of templates that match entity requirements
            patterns: List of similar query patterns
            
        Returns:
            Scored list of templates
        """
        # Extract keywords from the query
        query_words = query.lower().split()
        keywords = [word for word in query_words if len(word) > 3]  # Filter out short words
        
        # Score templates based on keyword matches
        scored_templates = []
        for template in templates:
            template_keywords = template.get("keywords", [])
            
            # Calculate keyword score
            keyword_score = sum(1 for k in keywords if k.lower() in template_keywords)
            
            # Calculate pattern score
            pattern_score = 0
            for pattern in patterns:
                if template.get("id") in pattern.get("template_ids", []):
                    pattern_score += pattern.get("score", 0)
            
            # Combined score
            combined_score = keyword_score * 0.5 + pattern_score * 0.5
            
            if combined_score > 0:
                scored_templates.append({
                    "template": template,
                    "keyword_score": keyword_score,
                    "pattern_score": pattern_score,
                    "combined_score": combined_score
                })
        
        # Sort by combined score
        scored_templates.sort(key=lambda x: x["combined_score"], reverse=True)
        
        # Format for return
        return [
            {
                **item["template"],
                "score": item["combined_score"]
            } for item in scored_templates
        ]
    
    def _rerank_templates(
        self, 
        query: str, 
        scored_templates: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Rerank templates using cross-encoder model.
        
        Args:
            query: The refined user query
            scored_templates: List of scored templates
            
        Returns:
            Reranked list of templates
        """
        try:
            # Prepare pairs for reranking
            pairs = []
            for template in scored_templates:
                # Create a descriptive text for the template
                template_text = f"{template.get('name', '')}: {template.get('description', '')}"
                pairs.append((query, template_text))
            
            # Get scores from reranking model
            rerank_scores = self.reranking_model.rerank(pairs)
            
            # Assign new scores to templates
            reranked_templates = []
            for i, template in enumerate(scored_templates):
                reranked_templates.append({
                    **template,
                    "initial_score": template.get("score", 0),
                    "rerank_score": rerank_scores[i]
                })
            
            # Sort by rerank score
            reranked_templates.sort(key=lambda x: x["rerank_score"], reverse=True)
            
            # Format for return
            return [
                {
                    **item,
                    "score": item["rerank_score"]
                } for item in reranked_templates
            ]
        except Exception as e:
            print(f"Error in template reranking: {e}")
            return scored_templates  # Return original scoring if reranking fails
    
    def index_query_pattern(
        self, 
        pattern: Dict[str, Any], 
        embedding_model: Optional[BiEncoderModel] = None
    ) -> bool:
        """
        Index a query pattern in the vector database.
        
        Args:
            pattern: Query pattern data
            embedding_model: Optional embedding model to use
            
        Returns:
            True if successful, False otherwise
        """
        try:
            model = embedding_model or self.embedding_model
            
            # Prepare point data
            pattern_text = f"{pattern.get('pattern', '')} {' '.join(pattern.get('keywords', []))}"
            
            # Generate embedding
            vector = model.embed(pattern_text)
            
            # Prepare payload
            payload = {
                "pattern": pattern.get("pattern", ""),
                "template_ids": pattern.get("template_ids", []),
                "keywords": pattern.get("keywords", [])
            }
            
            # Upsert to vector database
            self.qdrant_client.upsert_points(
                collection_name=self.patterns_collection,
                points=[
                    {
                        "id": pattern.get("id", str(hash(pattern_text))),
                        "vector": vector,
                        "payload": payload
                    }
                ]
            )
            
            return True
        except Exception as e:
            print(f"Error indexing query pattern: {e}")
            return False
    
    def index_sparql_example(
        self, 
        example: Dict[str, Any], 
        embedding_model: Optional[BiEncoderModel] = None
    ) -> bool:
        """
        Index a SPARQL query example in the vector database.
        
        Args:
            example: SPARQL example data
            embedding_model: Optional embedding model to use
            
        Returns:
            True if successful, False otherwise
        """
        try:
            model = embedding_model or self.embedding_model
            
            # Prepare text for embedding
            example_text = example.get("natural_language", "")
            
            # Generate embedding
            vector = model.embed(example_text)
            
            # Prepare payload
            payload = {
                "natural_language": example.get("natural_language", ""),
                "sparql": example.get("sparql", ""),
                "entities": example.get("entities", {}),
                "template_id": example.get("template_id", "")
            }
            
            # Upsert to vector database
            self.qdrant_client.upsert_points(
                collection_name=self.examples_collection,
                points=[
                    {
                        "id": example.get("id", str(hash(example_text))),
                        "vector": vector,
                        "payload": payload
                    }
                ]
            )
            
            return True
        except Exception as e:
            print(f"Error indexing SPARQL example: {e}")
            return False
