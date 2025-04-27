import json
from typing import Any, Dict, List, Optional

import autogen

from database.elastic_client import ElasticClient
from utils.constants import QDRANT_CLIENT_SINGLETON


class ToolSelectionAgent:
    """
    Agent for selecting appropriate tools for handling NL queries
    Uses vector similarity to match queries with known patterns
    """
    
    def __init__(
        self, 
        elastic_client: ElasticClient
    ):
        """
        Initialize the tool selection agent.
        
        Args:
            elastic_client: Client for document search
        """
        # Store clients
        self.elastic_client = elastic_client
        
        # Get configuration for tool selection agent
        self.tools = self._initialize_tools()
        
        # Initialize the autogen agent
        self.agent = autogen.AssistantAgent(
            name="ToolSelector",
            system_message="You are an AI assistant that helps select appropriate tools for processing natural language queries.",
            llm_config={"config_list": [{"model": "gpt-3.5-turbo"}]}
        )

    def select_tools(self, query: str) -> List[str]:
        """
        Select appropriate tools for handling a query.
        
        Args:
            query: The natural language query
            
        Returns:
            List of selected tool names
        """
        try:
            # Get similar patterns from vector DB
            results = QDRANT_CLIENT_SINGLETON.search(
                collection_name="query_patterns",
                query_text=query,
                limit=3
            )
            
            # Extract tools from matched patterns
            selected_tools = []
            for result in results:
                pattern = result["payload"]["pattern"]
                tools = self._get_tools_for_pattern(pattern)
                selected_tools.extend(tools)
                
            # Remove duplicates while preserving order
            selected_tools = list(dict.fromkeys(selected_tools))
            
            return selected_tools
        except Exception as e:
            print(f"Error selecting tools: {e}")
            # Return default tool chain
            return ["query_refinement", "entity_recognition", "sparql_construction"]

    def _initialize_tools(self) -> Dict[str, Dict]:
        """Initialize available tools and their metadata."""
        return {
            "query_refinement": {
                "description": "Refines ambiguous queries into clear questions",
                "patterns": [
                    "what does * mean",
                    "tell me more about *",
                    "can you explain *"
                ]
            },
            "entity_recognition": {
                "description": "Identifies entities in natural language",
                "patterns": [
                    "show me *",
                    "find * where *",
                    "what * have *"
                ]
            },
            "sparql_construction": {
                "description": "Constructs SPARQL queries",
                "patterns": [
                    "list all *",
                    "count how many *",
                    "what is the * of *"
                ]
            }
        }

    def _get_tools_for_pattern(self, pattern: str) -> List[str]:
        """Get relevant tools for a query pattern."""
        matching_tools = []
        for tool_name, tool_info in self.tools.items():
            for tool_pattern in tool_info["patterns"]:
                if self._pattern_matches(pattern, tool_pattern):
                    matching_tools.append(tool_name)
                    break
        return matching_tools

    def _pattern_matches(self, query: str, pattern: str) -> bool:
        """Check if a query matches a pattern."""
        # Simple wildcard matching
        parts = pattern.split("*")
        if len(parts) == 1:
            return query == pattern
            
        current_pos = 0
        for part in parts:
            if part:
                pos = query.find(part, current_pos)
                if pos == -1:
                    return False
                current_pos = pos + len(part)
                
        return True
