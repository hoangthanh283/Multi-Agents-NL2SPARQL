from abc import ABC, abstractmethod
from typing import Dict, Any

class AbstractSlave(ABC):
    """
    Abstract interface for all slave components in the Master-Slave architecture.
    Slaves are specialized workers that perform specific tasks within a domain.
    Each slave wraps an existing agent to adapt it to the slave interface.
    """
    
    @abstractmethod
    def execute_task(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a task using this slave's specialized capability.
        
        Args:
            parameters: Task parameters specific to this slave type
            
        Returns:
            Dictionary with task execution results
        """
        pass
    
    @abstractmethod
    def report_status(self) -> Dict[str, Any]:
        """
        Report the current status of this slave.
        
        Returns:
            Dictionary with status information
        """
        pass
    
    @abstractmethod
    def get_health(self) -> bool:
        """
        Check if this slave is healthy and operational.
        
        Returns:
            Boolean indicating health status
        """
        pass