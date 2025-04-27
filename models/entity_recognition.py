import re
from typing import Any, Dict, List, Optional

import torch
from gliner import GLiNER

GLINER_AVAILABLE = True


class GLiNERModel:
    """
    GLiNER model for named entity recognition in SPARQL queries.
    Uses the Generalist model adapted for ontology-related entities.
    """
    def __init__(
        self, 
        model_name_or_path: str = "urchade/gliner_medium-v2.1",
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
            
        # Load GLiNER model
        try:
            if not GLINER_AVAILABLE:
                raise ImportError("GLiNER package not available")
                
            self.model = GLiNER.from_pretrained(model_name_or_path)
            self.model.to(self.device)
            print(f"Loaded GLiNER model on {self.device}")
            self.model_loaded = True
        except Exception as e:
            print(f"Error loading GLiNER model: {e}")
            self.model = None
            self.model_loaded = False
        
        # Initialize entity type prompts mapping
        self.entity_type_prompts = {
            "CLASS": "Class",
            "PROPERTY": "Property",
            "INSTANCE": "Instance",
            "LITERAL": "Literal",
            "RELATION": "Relation",
            "FILTER": "Filter",
            "QUERY_TYPE": "QueryType"
        }
        
        # Additional regex patterns for fallback
        self.regex_patterns = {
            "CLASS": [
                r'\b(Person|Organization|Publication|Article|Book|Researcher|Professor|Student|University|Department|Class)\b'
            ],
            "PROPERTY": [
                r'\b(name|title|email|author|publication|date|location|address|property|has|is|of|with)\b'
            ],
            "LITERAL": [
                r'\b\d+(?:\.\d+)?\b',  # Numbers
                r'\b\d{4}-\d{2}-\d{2}\b',  # Dates
                r'"([^"]*)"'  # Quoted strings
            ],
            "QUERY_TYPE": [
                r'\b(SELECT|ASK|CONSTRUCT|DESCRIBE)\b'
            ],
            "FILTER": [
                r'\b(greater than|less than|equal to|contains|starting with|ending with)\b',
                r'\b(>|<|=|>=|<=)\b'
            ]
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
        # Use all entity types if none specified
        if entity_types is None:
            entity_types = list(self.entity_type_prompts.keys())
            
        # Filter to valid entity types
        entity_types = [et for et in entity_types if et in self.entity_type_prompts]
        
        # If GLiNER model not loaded, fall back to regex-based extraction
        if not self.model_loaded or self.model is None:
            print("Warning: Using fallback entity extraction (model not loaded)")
            return self._fallback_entity_extraction(text, entity_types, confidence_threshold)
        
        try:
            # Convert entity types to GLiNER compatible format
            gliner_labels = [self.entity_type_prompts[et] for et in entity_types]
            
            # Use GLiNER predict_entities method
            predicted_entities = self.model.predict_entities(
                text=text,
                labels=gliner_labels,
                threshold=confidence_threshold
            )
            
            # Convert GLiNER format to our expected format
            formatted_entities = []
            for entity in predicted_entities:
                # Map back from GLiNER label to our entity type
                label_to_type = {v: k for k, v in self.entity_type_prompts.items()}
                entity_type = label_to_type.get(entity["label"], "UNKNOWN")
                
                # Only include entities of requested types
                if entity_type in entity_types:
                    formatted_entities.append({
                        "entity_text": entity["text"],
                        "entity_type": entity_type,
                        "start_position": entity["start"],
                        "end_position": entity["end"],
                        "confidence": entity["score"]
                    })
            
            # Sort entities by their start position
            formatted_entities.sort(key=lambda x: x.get("start_position", 0))
            
            # Add SPARQL-specific entities via regex that GLiNER might miss
            sparql_entities = self._extract_sparql_specific_entities(text, confidence_threshold)
            
            # Combine GLiNER entities with SPARQL-specific entities
            all_entities = formatted_entities.copy()
            for sparql_entity in sparql_entities:
                # Only add if it doesn't overlap with existing entities
                if not self._is_overlapping(sparql_entity, formatted_entities):
                    all_entities.append(sparql_entity)
            
            # Re-sort combined entities
            all_entities.sort(key=lambda x: x.get("start_position", 0))
            
            return all_entities
        
        except Exception as e:
            print(f"Error using GLiNER model: {e}")
            return self._fallback_entity_extraction(text, entity_types, confidence_threshold)
    
    def _fallback_entity_extraction(
        self, 
        text: str, 
        entity_types: List[str] = None,
        threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Fallback entity extraction using regex when model isn't available.
        
        Args:
            text: Text to extract entities from
            entity_types: List of entity types to extract
            threshold: Confidence threshold for regex matches
            
        Returns:
            List of extracted entities
        """
        entities = []
        
        # Use all entity types if none specified
        if entity_types is None:
            entity_types = list(self.regex_patterns.keys())
        
        # Extract entities using patterns
        for entity_type in entity_types:
            if entity_type not in self.regex_patterns:
                continue
                
            for pattern in self.regex_patterns[entity_type]:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    # Get the matched text
                    if entity_type == "LITERAL" and pattern == r'"([^"]*)"':
                        # For quoted strings, get content without quotes
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
                        "confidence": threshold,
                        "source": "regex"
                    })
        
        # Add SPARQL-specific entities
        sparql_entities = self._extract_sparql_specific_entities(text, threshold)
        entities.extend(sparql_entities)
        entities.sort(key=lambda x: x.get("start_position", 0))
        return entities
    
    def _extract_sparql_specific_entities(
        self, 
        text: str, 
        threshold: float
    ) -> List[Dict[str, Any]]:
        """
        Extract SPARQL-specific entities.
        
        Args:
            text: Text to extract entities from
            threshold: Confidence threshold
            
        Returns:
            List of extracted SPARQL-specific entities
        """
        entities = []
        
        # SPARQL query types (SELECT, ASK, CONSTRUCT, DESCRIBE)
        query_types = ["SELECT", "ASK", "CONSTRUCT", "DESCRIBE"]
        for qt in query_types:
            for match in re.finditer(r'\b' + qt + r'\b', text, re.IGNORECASE):
                entity_text = match.group(0)
                start_pos = match.start()
                end_pos = match.end()
                
                entities.append({
                    "entity_text": entity_text,
                    "entity_type": "QUERY_TYPE",
                    "start_position": start_pos,
                    "end_position": end_pos,
                    "confidence": 0.95,  # High confidence for exact matches
                    "source": "sparql"
                })
        
        # Filter conditions
        filter_patterns = {
            r'\b(?:greater\s+than|more\s+than|above)\b': "greater_than",
            r'\b(?:less\s+than|fewer\s+than|below)\b': "less_than",
            r'\b(?:equal\s+to|equals|is|=)\b': "equal_to",
            r'\b(?:contains|including|with)\b': "contains",
            r'\b(?:starting\s+with|begins\s+with|starts\s+with)\b': "starts_with",
            r'\b(?:ending\s+with|ends\s+with)\b': "ends_with"
        }
        
        for pattern, filter_type in filter_patterns.items():
            for match in re.finditer(pattern, text, re.IGNORECASE):
                entity_text = match.group(0)
                start_pos = match.start()
                end_pos = match.end()
                
                entities.append({
                    "entity_text": entity_text,
                    "entity_type": "FILTER",
                    "start_position": start_pos,
                    "end_position": end_pos,
                    "confidence": 0.9,
                    "filter_type": filter_type,
                    "source": "sparql"
                })
        
        return entities
    
    def _is_overlapping(
        self, 
        entity: Dict[str, Any], 
        entities: List[Dict[str, Any]]
    ) -> bool:
        """
        Check if an entity overlaps with any in a list of entities.
        
        Args:
            entity: Entity to check
            entities: List of entities to check against
            
        Returns:
            True if there's an overlap, False otherwise
        """
        start = entity.get("start_position", 0)
        end = entity.get("end_position", 0)
        
        for other in entities:
            other_start = other.get("start_position", 0)
            other_end = other.get("end_position", 0)
            
            # Check for overlap
            if start <= other_end and end >= other_start:
                return True
        
        return False


class EntityRecognitionModel:
    """
    Model for entity recognition in natural language text.
    This is a placeholder class that can be implemented with actual NER models.
    """
    
    def __init__(self, model_name="default", config=None):
        """
        Initialize the entity recognition model.
        
        Args:
            model_name: Name of the model to use
            config: Configuration parameters for the model
        """
        self.model_name = model_name
        self.config = config or {}
        self.is_initialized = True
    
    def recognize(self, text):
        """
        Recognize entities in the given text.
        
        Args:
            text: The input text to process
            
        Returns:
            Dict of recognized entities by type
        """
        # This is a placeholder implementation
        # In a real implementation, this would use NLP models to identify entities
        return {
            "diseases": [],
            "medications": [],
            "symptoms": [],
            "procedures": []
        }
    
    def is_ready(self):
        """
        Check if the model is ready for inference.
        
        Returns:
            Boolean indicating if the model is ready
        """
        return self.is_initialized
