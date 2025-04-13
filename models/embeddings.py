from typing import Any, Dict, List, Tuple, Union

import numpy as np
from sentence_transformers import CrossEncoder, SentenceTransformer


class EmbeddingModel:
    """
    Base class for embedding models.
    Provides interfaces for generating embeddings and ranking.
    """
    
    def __init__(self, model_name_or_path: str):
        """
        Initialize the embedding model.
        
        Args:
            model_name_or_path: Name or path of the model
        """
        self.model_name = model_name_or_path
    
    def embed(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """
        Generate embeddings for text.
        
        Args:
            text: Text or list of texts to embed
            
        Returns:
            Embeddings as a list of floats or list of list of floats
        """
        raise NotImplementedError("Subclasses must implement embed method")
    
    def rerank(self, pairs: List[Tuple[str, str]]) -> List[float]:
        """
        Rerank text pairs.
        
        Args:
            pairs: List of (query, document) text pairs
            
        Returns:
            List of similarity scores
        """
        raise NotImplementedError("Subclasses must implement rerank method")


class BiEncoderModel(EmbeddingModel):
    """
    Bi-encoder model for generating embeddings.
    Uses SentenceTransformer models.
    """
    
    def __init__(self, model_name_or_path: str = "all-MiniLM-L6-v2"):
        """
        Initialize the bi-encoder model.
        
        Args:
            model_name_or_path: Name or path of the SentenceTransformer model
        """
        super().__init__(model_name_or_path)
        
        # Load the model
        self.model = SentenceTransformer(model_name_or_path)
        
        # Maximum sequence length
        self.max_seq_length = self.model.max_seq_length
    
    def embed(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """
        Generate embeddings for text.
        
        Args:
            text: Text or list of texts to embed
            
        Returns:
            Embeddings as a list of floats or list of list of floats
        """
        # Ensure input is a list
        if isinstance(text, str):
            text = [text]
            
        # Generate embeddings
        embeddings = self.model.encode(text, convert_to_tensor=False)
        
        # Convert to lists
        embeddings = embeddings.tolist()
        
        # Return single embedding if input was a single string
        if len(text) == 1:
            return embeddings[0]
            
        return embeddings
    
    def rerank(self, pairs: List[Tuple[str, str]]) -> List[float]:
        """
        Approximate reranking using bi-encoder for comparison.
        
        Args:
            pairs: List of (query, document) text pairs
            
        Returns:
            List of similarity scores
        """
        # Extract queries and documents from pairs
        queries = [pair[0] for pair in pairs]
        documents = [pair[1] for pair in pairs]
        
        # Generate embeddings
        query_embeddings = self.model.encode(queries, convert_to_tensor=True)
        doc_embeddings = self.model.encode(documents, convert_to_tensor=True)
        
        # Compute cosine similarities
        from torch.nn.functional import cosine_similarity
        scores = cosine_similarity(query_embeddings.unsqueeze(1), doc_embeddings.unsqueeze(0), dim=2)
        
        # Convert to list and normalize to 0-1 range
        scores = scores.diagonal().cpu().numpy().tolist()
        
        # Normalize scores to [0, 1] range
        scores = [(score + 1) / 2 for score in scores]
        
        return scores


class CrossEncoderModel(EmbeddingModel):
    """
    Cross-encoder model for reranking.
    Uses SentenceTransformer CrossEncoder models.
    """
    
    def __init__(self, model_name_or_path: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"):
        """
        Initialize the cross-encoder model.
        
        Args:
            model_name_or_path: Name or path of the CrossEncoder model
        """
        super().__init__(model_name_or_path)
        
        # Load the model
        self.model = CrossEncoder(model_name_or_path)
    
    def embed(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """
        Cross-encoders don't generate embeddings, so this is not implemented.
        
        Args:
            text: Text or list of texts to embed
            
        Returns:
            Raises NotImplementedError
        """
        raise NotImplementedError("Cross-encoder models do not generate embeddings")
    
    def rerank(self, pairs: List[Tuple[str, str]]) -> List[float]:
        """
        Rerank text pairs using cross-encoder model.
        
        Args:
            pairs: List of (query, document) text pairs
            
        Returns:
            List of similarity scores
        """
        # Score the pairs
        scores = self.model.predict(pairs)
        
        # Convert numpy array to list
        if isinstance(scores, np.ndarray):
            scores = scores.tolist()
            
        # Ensure scores are in [0, 1] range for cross-encoders that output logits
        if self.model.config.num_labels == 1:
            from scipy.special import expit  # sigmoid function
            scores = [float(expit(score)) for score in scores]
            
        return scores


class LLMEmbedderModel(BiEncoderModel):
    """
    LLM-Embedder model as described in the paper.
    Extends BiEncoderModel with task-specific instructions.
    """
    
    def __init__(
        self, 
        model_name_or_path: str = "BAAI/bge-large-en-v1.5"
    ):
        """
        Initialize the LLM-Embedder model.
        
        Args:
            model_name_or_path: Name or path of the SentenceTransformer model
        """
        super().__init__(model_name_or_path)
        
        # Task-specific instructions
        self.task_instructions = {
            "conversation_history": {
                "query": "Convert the following dialogue into vector to find relevant conversation history: ",
                "key": "Convert this conversation into vector for retrieval: "
            },
            "refinement_examples": {
                "query": "Convert the following dialogue into vector to find useful examples: ",
                "key": "Convert this example into vector for retrieval: "
            },
            "tool_selection": {
                "query": "Convert this query into vector to find relevant tools: ",
                "key": "Convert this tool description into vector for retrieval: "
            }
        }
        
        # Default task
        self.current_task = "tool_selection"
    
    def set_task(self, task_name: str):
        """
        Set the current task for task-specific embeddings.
        
        Args:
            task_name: Name of the task
        """
        if task_name not in self.task_instructions:
            raise ValueError(f"Unknown task: {task_name}")
            
        self.current_task = task_name
    
    def embed(self, text: Union[str, List[str]], role: str = "query") -> Union[List[float], List[List[float]]]:
        """
        Generate task-specific embeddings for text.
        
        Args:
            text: Text or list of texts to embed
            role: Either "query" or "key" depending on the role in the task
            
        Returns:
            Embeddings as a list of floats or list of list of floats
        """
        # Get the appropriate instruction for this task and role
        if role not in ["query", "key"]:
            raise ValueError(f"Unknown role: {role}")
            
        instruction = self.task_instructions[self.current_task][role]
        
        # Ensure input is a list
        if isinstance(text, str):
            text = [text]
            
        # Prepend instructions to each text
        instructed_text = [f"{instruction}{t}" for t in text]
        
        # Generate embeddings
        embeddings = self.model.encode(instructed_text, convert_to_tensor=False)
        
        # Convert to lists
        embeddings = embeddings.tolist()
        
        # Return single embedding if input was a single string
        if len(text) == 1:
            return embeddings[0]
            
        return embeddings
