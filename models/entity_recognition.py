import re
from typing import Any, Dict, List, Optional, Tuple

import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer


class GLiNERModel:
    """
    GLiNER model for named entity recognition in SPARQL queries.
    Uses the Generalist model adapted for ontology-related entities.
    """
    
    def __init__(
        self, 
        model_name_or_path: str = "urchade/gliner_small-v1",
        device: Optional[str] = None
    ):
        """
        Initialize the GLiNER model.
        
        Args:
            model_name_or_path: Name or path of the model
            device: Device to run the model on (cpu, cuda)
        """
        self.model_name = model_name_or_path
        
        # Determine device
        if device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device
            
        # Load model and tokenizer.
        try:
            self.model = AutoModelForTokenClassification.from_pretrained(model_name_or_path)
            self.tokenizer = AutoTokenizer.from_pretrained(model_name_or_path)
            
            # Move model to device
            self.model.to(self.device)
            print(f"Loaded GLiNER model on {self.device}")
        except Exception as e:
            print(f"Error loading GLiNER model: {e}")
            # Create dummy attributes for graceful degradation
            self.model = None
            self.tokenizer = None
        
        # Initialize entity type prompts mapping
        self.entity_type_prompts = {
            "CLASS": "Extract ontology classes",
            "PROPERTY": "Extract ontology properties",
            "INSTANCE": "Extract instances or individuals",
            "LITERAL": "Extract literal values like numbers, dates, and strings",
            "RELATION": "Extract relationships between entities",
            "FILTER": "Extract filter conditions",
            "QUERY_TYPE": "Extract query type (SELECT, ASK, CONSTRUCT, DESCRIBE)"
        }
    
    def extract_entities(
        self, 
        text: str, 
        entity_types: Optional[List[str]] = None, 
        confidence_threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Extract entities from text.
        
        Args:
            text: Text to extract entities from
            entity_types: List of entity types to extract
            confidence_threshold: Minimum confidence score for entities
            
        Returns:
            List of extracted entities with their types and positions
        """
        # Handle case where model isn't loaded properly
        if self.model is None or self.tokenizer is None:
            print("Warning: Using fallback entity extraction (model not loaded)")
            return self._fallback_entity_extraction(text)
        
        # Use all entity types if none specified
        if entity_types is None:
            entity_types = list(self.entity_type_prompts.keys())
            
        # Filter to valid entity types
        entity_types = [et for et in entity_types if et in self.entity_type_prompts]
        
        # Extract entities for each type
        all_entities = []
        for entity_type in entity_types:
            prompt = self.entity_type_prompts[entity_type]
            entities = self._extract_entities_for_type(text, prompt, entity_type, confidence_threshold)
            all_entities.extend(entities)
            
        # Sort entities by their start position
        all_entities.sort(key=lambda x: x.get("start_position", 0))
        
        return all_entities
    
    def _extract_entities_for_type(
        self, 
        text: str, 
        prompt: str, 
        entity_type: str, 
        confidence_threshold: float
    ) -> List[Dict[str, Any]]:
        """
        Extract entities of a specific type from text.
        
        Args:
            text: Text to extract entities from
            prompt: Prompt for the entity type
            entity_type: Type of entities to extract
            confidence_threshold: Minimum confidence score for entities
            
        Returns:
            List of extracted entities with their types and positions
        """
        try:
            # Combine prompt and text
            prompt_text = f"{prompt}: {text}"
            
            # Tokenize
            inputs = self.tokenizer(
                prompt_text, 
                return_tensors="pt",
                truncation=True,
                max_length=512
            )
            
            # Move inputs to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Get model predictions
            with torch.no_grad():
                outputs = self.model(**inputs)
                
            # Get token-level predictions
            predictions = outputs.logits.argmax(dim=-1)[0].cpu().numpy()
            token_scores = torch.softmax(outputs.logits, dim=-1)[0].cpu().numpy()
            confidences = [token_scores[i, pred] for i, pred in enumerate(predictions)]
            
            # Get token offsets
            tokens = self.tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
            token_offsets = self._get_token_offsets(text, tokens, prompt)
            
            # Extract entities
            entities = []
            i = 0
            while i < len(predictions):
                # Check if token starts an entity (B- tag)
                if predictions[i] % 2 == 1:  # Odd numbers are B- tags in BIO scheme
                    start_idx = i
                    entity_tag = predictions[i] // 2
                    
                    # Find end of entity
                    j = i + 1
                    while j < len(predictions) and predictions[j] == entity_tag * 2:  # Even numbers are I- tags
                        j += 1
                        
                    # Get entity text and positions
                    entity_tokens = tokens[start_idx:j]
                    entity_confidence = sum(confidences[start_idx:j]) / len(confidences[start_idx:j])
                    
                    # Only include entities above confidence threshold
                    if entity_confidence >= confidence_threshold:
                        # Calculate start and end positions in original text
                        start_pos = token_offsets[start_idx][0] if start_idx < len(token_offsets) else None
                        end_pos = token_offsets[j-1][1] if j-1 < len(token_offsets) else None
                        
                        if start_pos is not None and end_pos is not None:
                            entity_text = text[start_pos:end_pos]
                            
                            entities.append({
                                "entity_text": entity_text,
                                "entity_type": entity_type,
                                "start_position": start_pos,
                                "end_position": end_pos,
                                "confidence": float(entity_confidence)
                            })
                    
                    i = j
                else:
                    i += 1
                    
            return entities
        except Exception as e:
            print(f"Error extracting entities: {e}")
            return []
    
    def _get_token_offsets(self, text: str, tokens: List[str], prompt: str) -> List[Tuple[int, int]]:
        """
        Get character offsets for each token in the original text.
        
        Args:
            text: Original text
            tokens: Tokenized text
            prompt: Prompt prepended to the text
            
        Returns:
            List of (start, end) character positions for each token
        """
        # Find the offset where the actual text starts (after the prompt)
        prompt_offset = len(f"{prompt}: ")
        
        # Filter out special tokens and prompt tokens
        filtered_tokens = []
        skipped_chars = 0
        
        for token in tokens:
            if token in ["[CLS]", "[SEP]", "[PAD]", "<s>", "</s>"]:
                continue
                
            # Skip tokens that are part of the prompt
            if skipped_chars < prompt_offset:
                token_len = len(token)
                if token.startswith("##"):
                    token_len = len(token) - 2
                
                skipped_chars += token_len
                if skipped_chars >= prompt_offset:
                    # This token spans the boundary between prompt and text
                    filtered_tokens.append(token)
            else:
                filtered_tokens.append(token)
        
        # Map tokens to their character offsets in the original text
        offsets = []
        current_pos = 0
        
        for token in filtered_tokens:
            # Handle subword tokens
            is_subword = token.startswith("##")
            if is_subword:
                token = token[2:]
                
            if not is_subword:
                # Skip whitespace for new tokens
                while current_pos < len(text) and text[current_pos].isspace():
                    current_pos += 1
            
            # Find the token in the text
            if current_pos < len(text):
                if current_pos + len(token) <= len(text):
                    # Simple case: token is the next substring
                    token_text = text[current_pos:current_pos + len(token)]
                    if token_text.lower() == token.lower():
                        offsets.append((current_pos, current_pos + len(token)))
                        current_pos += len(token)
                        continue
                
                # Complex case: search for the token
                search_pos = text[current_pos:].lower().find(token.lower())
                if search_pos >= 0:
                    start_pos = current_pos + search_pos
                    end_pos = start_pos + len(token)
                    offsets.append((start_pos, end_pos))
                    current_pos = end_pos
                    continue
            
            # Fallback: just use current position
            offsets.append((current_pos, current_pos + len(token)))
            current_pos += len(token)
        
        return offsets
    
    def _fallback_entity_extraction(self, text: str) -> List[Dict[str, Any]]:
        """
        Fallback entity extraction using regex when model isn't available.
        
        Args:
            text: Text to extract entities from
            
        Returns:
            List of extracted entities
        """
        entities = []
        
        # Simple patterns for entity types
        patterns = {
            "CLASS": [
                r'\b(Person|Organization|Publication|Article|Book|Researcher|Professor|Student|University|Department)\b'
            ],
            "PROPERTY": [
                r'\b(name|title|email|author|publication|date|location|address)\b'
            ],
            "LITERAL": [
                r'\b\d+(?:\.\d+)?\b',  # Numbers
                r'\b\d{4}-\d{2}-\d{2}\b',  # Dates
                r'"([^"]*)"'  # Quoted strings
            ],
            "QUERY_TYPE": [
                r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b'
            ]
        }
        
        # Extract entities using patterns
        for entity_type, type_patterns in patterns.items():
            for pattern in type_patterns:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    # Get the matched text
                    if entity_type == "LITERAL" and pattern == r'"([^"]*)"':
                        # For quoted strings, get the content without quotes
                        entity_text = match.group(1)
                        start_pos = match.start(1)
                        end_pos = match.end(1)
                    else:
                        entity_text = match.group(0)
                        start_pos = match.start()
                        end_pos = match.end()
                    
                    entities.append({
                        "entity_text": entity_text,
                        "entity_type": entity_type,
                        "start_position": start_pos,
                        "end_position": end_pos,
                        "confidence": 0.7  # Default confidence for regex matches
                    })
        
        return entities

# Optional additional model class for future extension
class CustomNERModel:
    """
    Custom NER model specifically trained for SPARQL/ontology domain.
    This is a placeholder for future implementation.
    """
    
    def __init__(self, model_path: str = "models/ner_model"):
        """
        Initialize the custom NER model.
        
        Args:
            model_path: Path to the model files
        """
        self.model_path = model_path
        self.model = None
        print(f"Warning: CustomNERModel is a placeholder and not yet implemented")
    
    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract entities from text.
        
        Args:
            text: Text to extract entities from
            
        Returns:
            List of extracted entities (empty placeholder)
        """
        # This is a placeholder
        return []
