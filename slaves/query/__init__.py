"""
Query domain slaves.

This package contains slave implementations for the Query domain:
- ontology_mapping_slave: For mapping entities to ontology concepts
- sparql_construction_slave: For constructing SPARQL queries from mapped entities
- validation_slave: For validating SPARQL queries
"""

from slaves.query.ontology_mapping_slave import OntologyMappingSlave
from slaves.query.sparql_construction_slave import SparqlConstructionSlave
from slaves.query.validation_slave import ValidationSlave

__all__ = [
    "OntologyMappingSlave",
    "SparqlConstructionSlave",
    "ValidationSlave"
]