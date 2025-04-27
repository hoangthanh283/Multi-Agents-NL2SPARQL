from typing import Any, Dict, List, Optional

import autogen
import numpy as np

from config.agent_config import get_agent_config
from models.embeddings import BiEncoderModel
from utils.constants import (QDRANT_CLIENT_SINGLETON,
                             VECTOR_SIMILARITY_THRESHOLD)


class QueryRefinementAgent:
    """
    Slave agent responsible for refining user queries into clear, standalone questions.
    Transforms ambiguous or context-dependent questions about knowledge graphs into 
    well-structured queries that can be processed by the system.
    """
    
    def __init__(self, embedding_model: Optional[BiEncoderModel] = None):
        """
        Initialize the query refinement agent.
        
        Args:
            embedding_model: Embedding model for semantic matching
        """
        # Get configuration for query refinement agent
        agent_config = get_agent_config("query_refinement")
        
        # Initialize the agent with AutoGen
        self.agent = autogen.AssistantAgent(
            name=agent_config["name"],
            system_message=agent_config["system_message"],
            llm_config=agent_config["llm_config"]
        )
        
        # Initialize proxy agent for interaction
        self.proxy = autogen.UserProxyAgent(
            name="QueryRefinementProxy",
            human_input_mode="NEVER",
            is_termination_msg=lambda x: True,  # Always terminate after one response
        )
        
        # Store or initialize embedding model
        self.embedding_model = embedding_model or BiEncoderModel()
        
        # Collection name for refinement examples
        self.examples_collection = "refinement_examples"
        
        # Collection name for conversation history
        self.history_collection = "conversation_history"
    
    def refine_query(self, raw_query: str, conversation_history: List[Dict]) -> str:
        """
        Refine a user query based on conversation history and similar examples.
        
        Args:
            raw_query: The original query from the user
            conversation_history: List of previous conversation messages
            
        Returns:
            Refined query as a standalone question
        """
        try:
            # Retrieve relevant conversation history
            relevant_history = self._get_relevant_history(raw_query, conversation_history)
            
            # Retrieve similar refinement examples
            similar_examples = self._get_similar_examples(raw_query)
            
            # Prepare the prompt for the LLM
            prompt = self._prepare_refinement_prompt(raw_query, relevant_history, similar_examples)
            
            # Get refined query from the LLM
            response = self.proxy.initiate_chat(
                self.agent,
                message=prompt
            )
            
            # Extract the refined query from the response.
            refined_query = response.summary.strip()
            
            # Clean up the response if needed (remove quotes, explanation text, etc.)
            if refined_query.startswith('"') and refined_query.endswith('"'):
                refined_query = refined_query[1:-1]
            
            # If the refinement failed, fall back to the original query
            if not refined_query:
                return raw_query
            return refined_query
        except Exception as e:
            logger.error(f"Error in query refinement: {e}")
            # Return the original query in case of any error
            return raw_query
    
    def _get_relevant_history(self, query: str, conversation_history: List[Dict], limit: int = 5) -> List[Dict]:
        """
        Retrieve relevant conversation history using vector similarity.
        
        Args:
            query: The current user query
            conversation_history: Full conversation history
            limit: Maximum number of history items to return
            
        Returns:
            List of relevant conversation history items
        """
        if not conversation_history:
            return []

        # If we have very few history items, just return them all
        if len(conversation_history) <= limit:
            return conversation_history
        
        try:
            # Vectorize the query
            query_embedding = self.embedding_model.embed(query)
            
            # Prepare the conversation history for vector search
            history_vectors = []
            for i, item in enumerate(conversation_history):
                if item.get("role") == "user":
                    content = item.get("content", "")
                    embedding = self.embedding_model.embed(content)
                    history_vectors.append({
                        "id": i,
                        "content": content,
                        "embedding": embedding
                    })
            
            # Calculate similarities
            similarities = []
            for item in history_vectors:
                similarity = self._cosine_similarity(query_embedding, item["embedding"])
                if similarity >= VECTOR_SIMILARITY_THRESHOLD:
                    similarities.append((item["id"], similarity))
            if similarities:
                return []

            # Sort by similarity.
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            # Get indices of most similar items.
            relevant_indices = [idx for idx, _ in similarities[:limit]]
            relevant_indices.sort()  # Maintain chronological order.
            
            # Get the corresponding history items
            relevant_history = [conversation_history[i] for i in relevant_indices]
            
            # Always include the most recent history items for context
            recent_items = conversation_history[-2:]  # Last 2 items
            for item in recent_items:
                if item not in relevant_history:
                    relevant_history.append(item)
            return relevant_history
        except Exception as e:
            print(f"Error retrieving relevant history: {e}")
            # Fall back to most recent history
            return conversation_history[-limit:]

    def _get_similar_examples(self, query: str, limit: int = 3) -> List[Dict[str, Any]]:
        """
        Retrieve similar refinement examples from the vector database.
        
        Args:
            query: The current user query
            limit: Maximum number of examples to return
            
        Returns:
            List of similar refinement examples
        """
        try:
            # Search for similar examples in the vector database
            search_results = QDRANT_CLIENT_SINGLETON.search(
                collection_name=self.examples_collection,
                query_text=query,
                embedding_model=self.embedding_model,
                limit=limit
            )
            
            # Format the results
            examples = []
            for result in search_results:
                examples.append({
                    "conversation_history": result.payload.get("conversation_history", ""),
                    "original_query": result.payload.get("original_query", ""),
                    "refined_query": result.payload.get("refined_query", "")
                })
                
            return examples
        except Exception as e:
            print(f"Error retrieving similar examples: {e}")
            # Return some default examples if retrieval fails
            return [
                {
                    "conversation_history": "User: What classes exist in the ontology? Assistant: The ontology contains classes like Person, Organization, Publication, etc.",
                    "original_query": "What are their properties?",
                    "refined_query": "What are the properties of the Person, Organization, and Publication classes in the ontology?"
                },
                {
                    "conversation_history": "User: Tell me about the Person class. Assistant: The Person class represents humans in the ontology with properties like name, email, etc.",
                    "original_query": "Show me some instances",
                    "refined_query": "Show me some instances of the Person class in the ontology."
                }
            ]
    
    def _prepare_refinement_prompt(
        self, 
        raw_query: str, 
        relevant_history: List[Dict], 
        similar_examples: List[Dict[str, Any]]
    ) -> str:
        """
        Prepare the prompt for the LLM to refine the query.
        
        Args:
            raw_query: The original query from the user
            relevant_history: Relevant conversation history
            similar_examples: Similar refinement examples
            
        Returns:
            Complete prompt for the LLM
        """
        # Format the conversation history
        history_text = ""
        for item in relevant_history:
            role = item.get("role", "")
            content = item.get("content", "")
            if role.lower() == "user":
                history_text += f"User: {content}\n"
            else:
                history_text += f"Assistant: {content}\n"
        
        # Format the examples
        examples_text = ""
        for i, example in enumerate(similar_examples, 1):
            examples_text += f"Example {i}:\n"
            examples_text += f"Conversation History: {example.get('conversation_history', '')}\n"
            examples_text += f"Original Query: {example.get('original_query', '')}\n"
            examples_text += f"Refined Query: {example.get('refined_query', '')}\n\n"
        
        # Construct the complete prompt
        prompt = f"""I need you to refine the following user query about a knowledge graph or ontology into a clear, standalone question.
Use the conversation history to fill in any missing context, resolve references, 
and make sure the query is complete and unambiguous.

Current Conversation History:
{history_text}

User Query: {raw_query}

Similar Examples of Query Refinement:
{examples_text}

Please provide the refined query as a standalone question. 
Do not include explanations or additional text, just the refined query.
Ensure the refined query would make sense even without the conversation history.
Include specific entity names whenever they're referenced directly or indirectly.
"""
        return prompt
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
            
        Returns:
            Cosine similarity score
        """
        # Convert to numpy arrays
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        # Calculate cosine similarity
        dot_product = np.dot(v1, v2)
        norm_v1 = np.linalg.norm(v1)
        norm_v2 = np.linalg.norm(v2)
        if norm_v1 == 0 or norm_v2 == 0:
            return 0.0
        return dot_product / (norm_v1 * norm_v2)
    
    def store_refinement_example(
        self, 
        conversation_history: str, 
        original_query: str, 
        refined_query: str
    ) -> bool:
        """
        Store a new refinement example in the vector database.
        
        Args:
            conversation_history: Formatted conversation history
            original_query: Original user query
            refined_query: Refined query
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare example data
            example_data = {
                "conversation_history": conversation_history,
                "original_query": original_query,
                "refined_query": refined_query
            }
            
            # Vectorize the original query for similarity search
            embedding = self.embedding_model.embed(original_query)
            
            # Generate a unique ID
            import hashlib
            example_id = hashlib.md5(f"{conversation_history}|{original_query}|{refined_query}".encode()).hexdigest()
            
            # Store the example in the vector database
            QDRANT_CLIENT_SINGLETON.upsert_points(
                collection_name=self.examples_collection,
                points=[
                    {
                        "id": example_id,
                        "vector": embedding,
                        "payload": example_data
                    }
                ]
            )
            return True
        except Exception as e:
            print(f"Error storing refinement example: {e}")
            return False
