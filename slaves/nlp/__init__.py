"""
NLP domain slaves.

This package contains slave implementations for the NLP domain:
- query_refinement_slave: For refining natural language queries
- entity_recognition_slave: For recognizing entities in natural language
"""

from slaves.nlp.query_refinement_slave import QueryRefinementSlave
from slaves.nlp.entity_recognition_slave import EntityRecognitionSlave

__all__ = [
    "QueryRefinementSlave",
    "EntityRecognitionSlave"
]