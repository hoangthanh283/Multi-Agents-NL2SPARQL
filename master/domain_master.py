"""
This module re-exports the DomainMaster class from base.py
to maintain backward compatibility with imports.
"""

from master.base import DomainMaster

# Re-export the DomainMaster class
__all__ = ['DomainMaster']