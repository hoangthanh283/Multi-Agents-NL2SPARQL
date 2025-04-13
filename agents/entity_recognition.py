import re
from typing import Any, Dict, List, Optional

from database.ontology_store import OntologyStore
from models.entity_recognition import GLiNERModel


class EntityRecognitionAgent:
    """
    Slave agent responsible for identifying knowledge graph entities in user queries.
    Uses a specialized entity recognition model combined with ontology lookups.
    """
    
    def __init__(
        self, 
        entity_recognition_model: GLiNERModel,
        ontology_store: OntologyStore
    ):
        """
        Initialize the entity recognition agent.
        
        Args:
            entity_recognition_model: Model for entity extraction
            ontology_store: Store for ontology access
        """
        self.model = entity_recognition_model
        self.ontology_store = ontology_store
        
        # Entity types we're interested in
        self.entity_types = [
            "CLASS",          # Ontology classes
            "PROPERTY",       # Ontology properties
            "INSTANCE",       # Specific instances
            "LITERAL",        # Literal values (strings, numbers, dates)
            "RELATION",       # Relationships between entities
            "FILTER",         # Filter conditions
            "QUERY_TYPE"      # Type of query (SELECT, ASK, etc.)
        ]
        
        # Common verbs and prepositions to clean up from entity text
        self.stopwords = [
            "a", "an", "the", "is", "are", "was", "were", "has", "have", 
            "had", "of", "in", "on", "at", "by", "for", "with", "about"
        ]
    
    def recognize_entities(self, query: str) -> Dict[str, Any]:
        """
        Extract entities from a refined query.
        
        Args:
            query: The refined user query
            
        Returns:
            Dictionary of identified entities and their types
        """
        # Step 1: Run the entity recognition model
        extracted_entities = self._extract_entities_with_model(query)
        
        # Step 2: Apply rule-based entity extraction to catch anything missed
        rule_based_entities = self._apply_rule_based_extraction(query)
        
        # Step 3: Merge the entities from both approaches
        merged_entities = self._merge_entities(extracted_entities, rule_based_entities)
        
        # Step 4: Organize entities by type
        organized_entities = self._organize_entities(merged_entities)
        
        # Step 5: Enrich with ontology information
        enriched_entities = self._enrich_with_ontology(organized_entities)
        
        return enriched_entities
    
    def _extract_entities_with_model(self, query: str) -> List[Dict[str, Any]]:
        """
        Extract entities using the entity recognition model.
        
        Args:
            query: The user query
            
        Returns:
            List of extracted entities with their types
        """
        try:
            # Use the GLiNER model to extract entities
            entity_predictions = self.model.extract_entities(
                text=query,
                entity_types=self.entity_types
            )
            
            # Format the extracted entities
            extracted_entities = []
            for entity_pred in entity_predictions:
                # Clean up the entity text
                entity_text = self._clean_entity_text(entity_pred["entity_text"])
                
                # Skip empty entities or stopwords
                if not entity_text or entity_text.lower() in self.stopwords:
                    continue
                
                extracted_entities.append({
                    "text": entity_text,
                    "type": entity_pred["entity_type"],
                    "start": entity_pred["start_position"],
                    "end": entity_pred["end_position"],
                    "confidence": entity_pred["confidence"],
                    "source": "model"
                })
                
            return extracted_entities
        except Exception as e:
            print(f"Error extracting entities with model: {e}")
            # Return an empty list if extraction fails
            return []
    
    def _apply_rule_based_extraction(self, query: str) -> List[Dict[str, Any]]:
        """
        Apply rule-based entity extraction to complement the model.
        
        Args:
            query: The user query
            
        Returns:
            List of extracted entities with their types
        """
        extracted_entities = []
        
        # Convert to lowercase for pattern matching
        query_lower = query.lower()
        
        # Rules for QUERY_TYPE detection
        query_type_patterns = {
            "SELECT": [r'\blist\b', r'\bshow\b', r'\bget\b', r'\bwhat\b', r'\bfind\b', r'\bsearch\b'],
            "ASK": [r'\bis there\b', r'\bdoes\b', r'\bdo\b', r'\bexists\b', r'\bwhether\b', r'\bcheck\b'],
            "CONSTRUCT": [r'\bcreate\b', r'\bconstruct\b', r'\bbuild\b', r'\bgenerate\b', r'\bmake\b'],
            "DESCRIBE": [r'\bdescribe\b', r'\bdetails\b', r'\binformation about\b', r'\btell me about\b']
        }
        
        # Check for query type indicators
        for query_type, patterns in query_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    extracted_entities.append({
                        "text": query_type,
                        "type": "QUERY_TYPE",
                        "confidence": 0.8,
                        "source": "rule"
                    })
                    break
                    
        # Detect literal values
        # Numbers
        number_matches = re.finditer(r'\b\d+(?:\.\d+)?\b', query)
        for match in number_matches:
            extracted_entities.append({
                "text": match.group(0),
                "type": "LITERAL",
                "start": match.start(),
                "end": match.end(),
                "confidence": 0.9,
                "source": "rule",
                "datatype": "xsd:decimal" if "." in match.group(0) else "xsd:integer"
            })
        
        # Dates
        date_matches = re.finditer(r'\b\d{4}-\d{2}-\d{2}\b', query)
        for match in date_matches:
            extracted_entities.append({
                "text": match.group(0),
                "type": "LITERAL",
                "start": match.start(),
                "end": match.end(),
                "confidence": 0.9,
                "source": "rule",
                "datatype": "xsd:date"
            })
        
        # Quoted strings
        quoted_matches = re.finditer(r'"([^"]*)"', query)
        for match in quoted_matches:
            extracted_entities.append({
                "text": match.group(1),
                "type": "LITERAL",
                "start": match.start() + 1,  # Skip the opening quote
                "end": match.end() - 1,      # Skip the closing quote
                "confidence": 0.9,
                "source": "rule",
                "datatype": "xsd:string"
            })
        
        # Detect filter conditions
        filter_patterns = [
            (r'\bgreater than\b|\bmore than\b|\babove\b|\b>\b', "greater_than"),
            (r'\bless than\b|\bfewer than\b|\bbelow\b|\b<\b', "less_than"),
            (r'\bequal to\b|\bequals\b|\bis\b|\b=\b', "equal_to"),
            (r'\bcontains\b|\bincluding\b', "contains"),
            (r'\bstarting with\b|\bbegins with\b|\bstarts with\b', "starts_with"),
            (r'\bending with\b|\bends with\b', "ends_with")
        ]
        
        for pattern, filter_type in filter_patterns:
            if re.search(pattern, query_lower):
                extracted_entities.append({
                    "text": filter_type,
                    "type": "FILTER",
                    "confidence": 0.8,
                    "source": "rule"
                })
        
        return extracted_entities
    
    def _merge_entities(
        self, 
        model_entities: List[Dict[str, Any]], 
        rule_entities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Merge entities from model and rule-based extraction.
        Resolve conflicts and duplicates.
        
        Args:
            model_entities: Entities from the model
            rule_entities: Entities from rule-based extraction
            
        Returns:
            Merged list of entities
        """
        all_entities = model_entities.copy()
        
        # For each rule-based entity, check if it overlaps with a model entity
        for rule_entity in rule_entities:
            # Skip rule entity if it doesn't have position info
            if "start" not in rule_entity or "end" not in rule_entity:
                # Check for duplicate by text and type
                is_duplicate = False
                for model_entity in model_entities:
                    if (model_entity["text"].lower() == rule_entity["text"].lower() and
                        model_entity["type"] == rule_entity["type"]):
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    all_entities.append(rule_entity)
                continue
            
            # Check for overlapping entities from the model
            overlapping = False
            for model_entity in model_entities:
                # Skip model entity if it doesn't have position info
                if "start" not in model_entity or "end" not in model_entity:
                    continue
                    
                # Check if there's an overlap
                if (rule_entity["start"] <= model_entity["end"] and 
                    rule_entity["end"] >= model_entity["start"]):
                    overlapping = True
                    
                    # If rule entity has higher confidence, replace the model entity
                    if rule_entity.get("confidence", 0) > model_entity.get("confidence", 0):
                        model_entity.update(rule_entity)
                    break
            
            # If no overlap, add the rule entity
            if not overlapping:
                all_entities.append(rule_entity)
        
        return all_entities
    
    def _organize_entities(self, entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Organize entities by type.
        
        Args:
            entities: List of extracted entities
            
        Returns:
            Dictionary of entities organized by type
        """
        organized = {
            entity_type.lower(): [] for entity_type in self.entity_types
        }
        
        for entity in entities:
            entity_type = entity["type"].lower()
            if entity_type in organized:
                organized[entity_type].append(entity)
        
        # Add a consolidated list of all entities
        organized["all_entities"] = entities
        
        return organized
    
    def _enrich_with_ontology(self, organized_entities: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich entities with information from the ontology.
        
        Args:
            organized_entities: Dictionary of organized entities
            
        Returns:
            Enriched entities with ontology information
        """
        enriched = organized_entities.copy()
        
        # Enrich class entities
        for entity in enriched.get("class", []):
            entity_text = entity["text"]
            
            # Search for matching classes in the ontology
            matching_classes = self.ontology_store.search_classes(entity_text)
            
            if matching_classes:
                # Add ontology information to the entity
                entity["ontology_matches"] = matching_classes
        
        # Enrich property entities
        for entity in enriched.get("property", []):
            entity_text = entity["text"]
            
            # Search for matching properties in the ontology
            matching_properties = self.ontology_store.search_properties(entity_text)
            
            if matching_properties:
                # Add ontology information to the entity
                entity["ontology_matches"] = matching_properties
        
        # Enrich instance entities
        for entity in enriched.get("instance", []):
            entity_text = entity["text"]
            
            # Search for matching instances in the ontology
            matching_instances = self.ontology_store.search_instances(entity_text)
            
            if matching_instances:
                # Add ontology information to the entity
                entity["ontology_matches"] = matching_instances
        
        return enriched
    
    def _clean_entity_text(self, text: str) -> str:
        """
        Clean up entity text by removing stopwords and extra whitespace.
        
        Args:
            text: Original entity text
            
        Returns:
            Cleaned entity text
        """
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove leading/trailing stopwords
        words = text.split()
        while words and words[0].lower() in self.stopwords:
            words.pop(0)
        while words and words[-1].lower() in self.stopwords:
            words.pop()
        return ' '.join(words)
