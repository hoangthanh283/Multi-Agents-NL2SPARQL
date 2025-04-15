import json
import os
from typing import Any, Dict, List, Optional

from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

class ToolRegistry:
    """
    Registry of available blockchain tools.
    Manages tool metadata, documentation, and execution.
    """
    
    def __init__(self, tools_dir: Optional[str] = None):
        """
        Initialize the tool registry.
        
        Args:
            tools_dir: Directory containing tool definitions
        """
        self.tools_dir = tools_dir or os.path.join(os.path.dirname(__file__), "definitions")
        self.tools = {}
        self.categories = {}
        
        # Load tool definitions
        self._load_tools()
    
    def _load_tools(self):
        """Load all tool definitions from the tools directory."""
        try:
            # Create tools directory if it doesn't exist
            if not os.path.exists(self.tools_dir):
                os.makedirs(self.tools_dir)
                logger.info(f"Created tools directory: {self.tools_dir}")
                
                # Create example tool definition
                self._create_example_tool()
            
            # Load all JSON files in the tools directory
            for filename in os.listdir(self.tools_dir):
                if filename.endswith(".json"):
                    tool_path = os.path.join(self.tools_dir, filename)
                    
                    with open(tool_path, "r") as f:
                        tool_data = json.load(f)
                        
                    tool_id = tool_data.get("id")
                    if not tool_id:
                        logger.warning(f"Tool definition missing ID: {filename}")
                        continue
                        
                    # Add tool to registry
                    self.tools[tool_id] = tool_data
                    
                    # Add tool to category
                    category = tool_data.get("category", "other")
                    if category not in self.categories:
                        self.categories[category] = []
                    self.categories[category].append(tool_id)
                    
            logger.info(f"Loaded {len(self.tools)} tools in {len(self.categories)} categories")
        except Exception as e:
            logger.error(f"Error loading tools: {e}")
    
    def _create_example_tool(self):
        """Create an example tool definition file."""
        example_tools = [
            {
                "id": "get_token_price",
                "name": "Get Token Price",
                "description": "Retrieves the current price of a cryptocurrency token.",
                "category": "information",
                "parameters": {
                    "token": {
                        "type": "string",
                        "description": "The name or symbol of the token (e.g., Bitcoin, BTC, ETH)",
                        "required": True
                    },
                    "currency": {
                        "type": "string",
                        "description": "The currency to display the price in (e.g., USD, EUR)",
                        "required": False,
                        "default": "USD"
                    }
                },
                "output": {
                    "price": {
                        "type": "number",
                        "description": "Current price of the token"
                    },
                    "currency": {
                        "type": "string",
                        "description": "Currency of the price"
                    },
                    "timestamp": {
                        "type": "string",
                        "description": "Timestamp of the price data"
                    }
                },
                "api_details": {
                    "endpoint": "https://api.example.com/v1/prices",
                    "method": "GET",
                    "params": {
                        "token": "{token}",
                        "currency": "{currency}"
                    },
                    "headers": {
                        "X-API-Key": "{API_KEY}"
                    }
                }
            },
            {
                "id": "get_trending_tokens",
                "name": "Get Trending Tokens",
                "description": "Retrieves a list of trending cryptocurrency tokens.",
                "category": "information",
                "parameters": {
                    "limit": {
                        "type": "number",
                        "description": "Number of trending tokens to return",
                        "required": False,
                        "default": 10
                    },
                    "timeframe": {
                        "type": "string",
                        "description": "Timeframe for trending data (e.g., 24h, 7d, 30d)",
                        "required": False,
                        "default": "24h"
                    }
                },
                "output": {
                    "trending": {
                        "type": "array",
                        "description": "List of trending tokens",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Name of the token"
                                },
                                "symbol": {
                                    "type": "string",
                                    "description": "Symbol of the token"
                                },
                                "price": {
                                    "type": "number",
                                    "description": "Current price in USD"
                                },
                                "change": {
                                    "type": "number",
                                    "description": "Price change percentage"
                                }
                            }
                        }
                    },
                    "timestamp": {
                        "type": "string",
                        "description": "Timestamp of the trending data"
                    }
                },
                "api_details": {
                    "endpoint": "https://api.example.com/v1/trending",
                    "method": "GET",
                    "params": {
                        "limit": "{limit}",
                        "timeframe": "{timeframe}"
                    },
                    "headers": {
                        "X-API-Key": "{API_KEY}"
                    }
                }
            },
            {
                "id": "deposit_tokens",
                "name": "Deposit Tokens",
                "description": "Deposits tokens into a lending pool.",
                "category": "transaction",
                "parameters": {
                    "token": {
                        "type": "string",
                        "description": "The token to deposit (e.g., DAI, USDC)",
                        "required": True
                    },
                    "amount": {
                        "type": "string",
                        "description": "Amount to deposit",
                        "required": True
                    },
                    "pool": {
                        "type": "string",
                        "description": "Lending pool to deposit into (e.g., Aave, Compound)",
                        "required": True
                    }
                },
                "output": {
                    "transaction": {
                        "type": "object",
                        "description": "Transaction details for deposit",
                        "properties": {
                            "to": {
                                "type": "string",
                                "description": "Contract address"
                            },
                            "data": {
                                "type": "string",
                                "description": "Encoded transaction data"
                            },
                            "value": {
                                "type": "string",
                                "description": "ETH value to send (usually 0 for token deposits)"
                            }
                        }
                    }
                },
                "api_details": {
                    "contract_abi": "deposit_function_signature",
                    "function_name": "deposit",
                    "parameters": [
                        "{token_address}",
                        "{amount_in_wei}",
                        "{user_address}",
                        "0"  # referral code
                    ]
                }
            }
        ]
        
        # Save each example tool to a separate file
        for tool in example_tools:
            tool_path = os.path.join(self.tools_dir, f"{tool['id']}.json")
            with open(tool_path, "w") as f:
                json.dump(tool, f, indent=2)
                
        logger.info(f"Created {len(example_tools)} example tool definitions")
    
    def get_tool(self, tool_id: str) -> Optional[Dict[str, Any]]:
        """
        Get tool definition by ID.
        
        Args:
            tool_id: ID of the tool
            
        Returns:
            Tool definition or None if not found
        """
        return self.tools.get(tool_id)
    
    def list_tools(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all tools, optionally filtered by category.
        
        Args:
            category: Category to filter by
            
        Returns:
            List of tool definitions
        """
        if category:
            tool_ids = self.categories.get(category, [])
            return [self.tools[tool_id] for tool_id in tool_ids]
        else:
            return list(self.tools.values())
    
    def get_tool_description(self, tool_id: str) -> str:
        """
        Get a formatted description of a tool.
        
        Args:
            tool_id: ID of the tool
            
        Returns:
            Formatted tool description
        """
        tool = self.get_tool(tool_id)
        if not tool:
            return f"Tool not found: {tool_id}"
            
        # Format parameters
        params_str = ""
        for param_name, param_info in tool.get("parameters", {}).items():
            required = param_info.get("required", False)
            default = param_info.get("default", "")
            
            req_str = "required" if required else f"optional, default: {default}"
            params_str += f"  - {param_name}: {param_info.get('description', '')} ({req_str})\n"
            
        # Create formatted description
        description = f"Tool: {tool.get('name')}\n"
        description += f"ID: {tool_id}\n"
        description += f"Category: {tool.get('category', 'other')}\n"
        description += f"Description: {tool.get('description', '')}\n"
        
        if params_str:
            description += f"Parameters:\n{params_str}"
            
        return description
    
    def register_tool(self, tool_definition: Dict[str, Any]) -> bool:
        """
        Register a new tool in the registry.
        
        Args:
            tool_definition: Tool definition
            
        Returns:
            True if successful, False otherwise
        """
        try:
            tool_id = tool_definition.get("id")
            if not tool_id:
                logger.error("Tool definition missing ID")
                return False
                
            # Save tool definition to file
            tool_path = os.path.join(self.tools_dir, f"{tool_id}.json")
            with open(tool_path, "w") as f:
                json.dump(tool_definition, f, indent=2)
                
            # Add to registry
            self.tools[tool_id] = tool_definition
            
            # Add to category
            category = tool_definition.get("category", "other")
            if category not in self.categories:
                self.categories[category] = []
            self.categories[category].append(tool_id)
            
            return True
        except Exception as e:
            logger.error(f"Error registering tool: {e}")
            return False
    
    def unregister_tool(self, tool_id: str) -> bool:
        """
        Unregister a tool from the registry.
        
        Args:
            tool_id: ID of the tool
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if tool_id not in self.tools:
                logger.warning(f"Tool not found: {tool_id}")
                return False
                
            # Remove tool file
            tool_path = os.path.join(self.tools_dir, f"{tool_id}.json")
            if os.path.exists(tool_path):
                os.remove(tool_path)
                
            # Remove from registry
            tool = self.tools.pop(tool_id)
            
            # Remove from category
            category = tool.get("category", "other")
            if category in self.categories and tool_id in self.categories[category]:
                self.categories[category].remove(tool_id)
                
            return True
        except Exception as e:
            logger.error(f"Error unregistering tool: {e}")
            return False
    
    def get_tool_vector_data(self) -> List[Dict[str, Any]]:
        """
        Get tool data for vector embedding.
        
        Returns:
            List of tool data for embedding
        """
        vector_data = []
        
        for tool_id, tool in self.tools.items():
            # Prepare text for embedding
            description = tool.get("description", "")
            name = tool.get("name", "")
            
            # Include parameter descriptions
            param_descriptions = []
            for param_name, param_info in tool.get("parameters", {}).items():
                param_descriptions.append(f"{param_name}: {param_info.get('description', '')}")
                
            # Combine all text
            text = f"{name}. {description}. "
            if param_descriptions:
                text += "Parameters: " + ". ".join(param_descriptions)
                
            # Add to vector data
            vector_data.append({
                "id": tool_id,
                "text": text,
                "payload": {
                    "tool_id": tool_id,
                    "tool_name": name,
                    "description": description,
                    "parameters": tool.get("parameters", {}),
                    "category": tool.get("category", "other")
                }
            })
            
        return vector_data
