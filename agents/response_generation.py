import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from tools.tool_registry import ToolRegistry

# Configure logging
logger = logging.getLogger(__name__)

class ToolExecutionAgent:
    """
    Slave agent responsible for executing blockchain API calls.
    Handles API authentication, error handling, and result formatting.
    """
    
    def __init__(self, tool_registry: Optional[ToolRegistry] = None):
        """
        Initialize the tool execution agent.
        
        Args:
            tool_registry: Registry of available tools
        """
        # Initialize tool registry
        self.tool_registry = tool_registry or ToolRegistry()
        
        # Cache for storing API results
        self.result_cache = {}
        
        # API keys for various blockchain services
        self.api_keys = {
            "coingecko": None,  # Will be loaded from environment
            "etherscan": None,
            "infura": None,
            # Add more API keys as needed
        }
        
        # Load API keys from environment
        self._load_api_keys()
    
    def _load_api_keys(self):
        """Load API keys from environment variables."""
        import os
        
        for api_name in self.api_keys.keys():
            env_var = f"{api_name.upper()}_API_KEY"
            self.api_keys[api_name] = os.getenv(env_var)
            
            if self.api_keys[api_name]:
                logger.info(f"Loaded API key for {api_name}")
            else:
                logger.warning(f"API key for {api_name} not found")
    
    def execute_tools(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute tools according to the execution plan.
        
        Args:
            execution_plan: The validated execution plan
            
        Returns:
            Dictionary of execution results
        """
        # Extract steps from the plan
        steps = execution_plan.get("steps", [])
        
        # If no steps, return empty results
        if not steps:
            return {"message": execution_plan.get("message", "No execution steps provided.")}
        
        # Initialize results dictionary
        results = {}
        
        # Track completed steps
        completed_steps = set()
        
        # Execute steps in order, respecting dependencies
        remaining_steps = list(steps)
        max_iterations = len(steps) * 2  # Avoid infinite loops
        iteration = 0
        
        while remaining_steps and iteration < max_iterations:
            iteration += 1
            next_step = None
            
            # Find the next step with all dependencies satisfied
            for i, step in enumerate(remaining_steps):
                step_num = step.get("step_number")
                dependencies = step.get("depends_on", [])
                
                # Check if all dependencies are completed
                if all(dep in completed_steps for dep in dependencies):
                    next_step = step
                    del remaining_steps[i]
                    break
            
            # If no eligible step found but steps remain, there's a dependency cycle
            if not next_step and remaining_steps:
                error_message = "Dependency cycle detected in execution plan"
                logger.error(error_message)
                results["error"] = error_message
                break
            
            # Execute the step
            if next_step:
                step_result = self._execute_step(next_step, results)
                step_num = next_step.get("step_number")
                
                # Store the result
                results[f"step_{step_num}"] = step_result
                completed_steps.add(step_num)
        
        # Check if any steps remained unexecuted
        if remaining_steps:
            results["warning"] = f"{len(remaining_steps)} steps could not be executed due to dependency issues"
        
        return results
    
    def _execute_step(self, step: Dict[str, Any], previous_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single step from the execution plan.
        
        Args:
            step: The step to execute
            previous_results: Results from previous steps
            
        Returns:
            Result of the step execution
        """
        tool_id = step.get("tool_id")
        parameters = step.get("parameters", {})
        step_num = step.get("step_number")
        
        logger.info(f"Executing step {step_num} with tool {tool_id}")
        
        # Get the tool definition
        tool_def = self.tool_registry.get_tool(tool_id)
        
        if not tool_def:
            error_message = f"Tool not found: {tool_id}"
            logger.error(error_message)
            return {"error": error_message}
        
        # Process parameter values
        processed_params = self._process_parameters(parameters, previous_results)
        
        # Execute the appropriate tool based on category
        category = tool_def.get("category", "other")
        
        if category == "information":
            return self._execute_information_tool(tool_def, processed_params)
        elif category == "transaction":
            return self._execute_transaction_tool(tool_def, processed_params)
        else:
            # Generic execution fallback
            return self._execute_generic_tool(tool_def, processed_params)
    
    def _process_parameters(
        self, 
        parameters: Dict[str, Any], 
        previous_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process parameter values, resolving references to previous results.
        
        Args:
            parameters: Original parameters
            previous_results: Results from previous steps
            
        Returns:
            Processed parameters
        """
        processed_params = {}
        
        for param_name, param_value in parameters.items():
            # Check if parameter references a previous result
            if isinstance(param_value, str) and param_value.startswith("${") and param_value.endswith("}"):
                ref_path = param_value[2:-1].split(".")
                ref_value = previous_results
                
                # Navigate the reference path
                for path_part in ref_path:
                    if path_part in ref_value:
                        ref_value = ref_value[path_part]
                    else:
                        logger.warning(f"Reference not found: {param_value}")
                        ref_value = param_value  # Use original as fallback
                        break
                
                processed_params[param_name] = ref_value
            else:
                processed_params[param_name] = param_value
        
        return processed_params
    
    def _execute_information_tool(
        self, 
        tool_def: Dict[str, Any], 
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute an information retrieval tool.
        
        Args:
            tool_def: Tool definition
            parameters: Processed parameters
            
        Returns:
            Tool execution result
        """
        tool_id = tool_def.get("id")
        api_details = tool_def.get("api_details", {})
        
        # Check if we have API details
        if not api_details:
            return {"error": "No API details available for this tool"}
        
        # Get API endpoint and method
        endpoint = api_details.get("endpoint")
        method = api_details.get("method", "GET")
        
        if not endpoint:
            return {"error": "No API endpoint specified"}
        
        # Generate cache key based on tool and parameters
        cache_key = f"{tool_id}_{json.dumps(parameters, sort_keys=True)}"
        
        # Check cache first
        if cache_key in self.result_cache:
            cache_entry = self.result_cache[cache_key]
            # Check if cache is still valid (less than 5 minutes old)
            if time.time() - cache_entry["timestamp"] < 300:
                return cache_entry["result"]
        
        # Format endpoint parameters
        endpoint = self._format_endpoint(endpoint, parameters)
        
        # Get query parameters
        query_params = {}
        for param_name, param_template in api_details.get("params", {}).items():
            param_value = self._format_value(param_template, parameters)
            if param_value:
                query_params[param_name] = param_value
        
        # Get headers
        headers = {}
        for header_name, header_template in api_details.get("headers", {}).items():
            header_value = self._format_value(header_template, parameters)
            if header_value:
                headers[header_name] = header_value
        
        # Add API key to headers or params as needed
        self._add_api_key(api_details, headers, query_params)
        
        try:
            # Make the API request
            response = requests.request(
                method=method,
                url=endpoint,
                params=query_params,
                headers=headers,
                timeout=10
            )
            
            # Check for HTTP errors
            response.raise_for_status()
            
            # Parse response
            if response.headers.get("content-type", "").startswith("application/json"):
                result = response.json()
            else:
                result = {"text": response.text}
            
            # Add metadata
            result["_metadata"] = {
                "http_status": response.status_code,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Cache the result
            self.result_cache[cache_key] = {
                "result": result,
                "timestamp": time.time()
            }
            
            return result
        
        except requests.exceptions.HTTPError as e:
            error_message = f"HTTP error: {e}"
            logger.error(error_message)
            return {"error": error_message, "status_code": e.response.status_code}
        
        except requests.exceptions.ConnectionError as e:
            error_message = f"Connection error: {e}"
            logger.error(error_message)
            return {"error": error_message}
        
        except requests.exceptions.Timeout as e:
            error_message = f"Timeout error: {e}"
            logger.error(error_message)
            return {"error": error_message}
        
        except requests.exceptions.RequestException as e:
            error_message = f"Request error: {e}"
            logger.error(error_message)
            return {"error": error_message}
        
        except Exception as e:
            error_message = f"Error executing tool: {e}"
            logger.error(error_message)
            return {"error": error_message}
    
    def _execute_transaction_tool(
        self, 
        tool_def: Dict[str, Any], 
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a blockchain transaction tool.
        For now, this just prepares transaction data without actual execution.
        
        Args:
            tool_def: Tool definition
            parameters: Processed parameters
            
        Returns:
            Transaction preparation result
        """
        api_details = tool_def.get("api_details", {})
        
        # Check if we have API details
        if not api_details:
            return {"error": "No API details available for this tool"}
        
        # For blockchain transactions, we prepare the transaction data
        # but don't execute it (execution requires wallet signature)
        try:
            # Get contract information
            contract_abi = api_details.get("contract_abi")
            function_name = api_details.get("function_name")
            contract_address = api_details.get("contract_address")
            
            # If contract_address is a parameter reference, resolve it
            if isinstance(contract_address, str) and contract_address.startswith("{") and contract_address.endswith("}"):
                param_name = contract_address[1:-1]
                contract_address = parameters.get(param_name)
            
            # Get function parameters
            function_params = []
            for param_template in api_details.get("parameters", []):
                param_value = self._format_value(param_template, parameters)
                function_params.append(param_value)
            
            # In a real implementation, we would encode the transaction data
            # For now, just return the parameters needed for a transaction
            transaction = {
                "to": contract_address,
                "function": function_name,
                "parameters": function_params,
                "abi": contract_abi,
                "_metadata": {
                    "description": "Transaction data to be signed by wallet",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
            return {"transaction": transaction}
        
        except Exception as e:
            error_message = f"Error preparing transaction: {e}"
            logger.error(error_message)
            return {"error": error_message}
    
    def _execute_generic_tool(
        self, 
        tool_def: Dict[str, Any], 
        parameters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a generic tool when category-specific handling is not available.
        
        Args:
            tool_def: Tool definition
            parameters: Processed parameters
            
        Returns:
            Tool execution result
        """
        # Simple execution result with metadata
        result = {
            "tool_id": tool_def.get("id"),
            "parameters": parameters,
            "message": "Generic tool execution completed",
            "_metadata": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        return result
    
    def _format_endpoint(self, endpoint: str, parameters: Dict[str, Any]) -> str:
        """
        Format an API endpoint with parameter values.
        
        Args:
            endpoint: Endpoint template with placeholders
            parameters: Parameter values
            
        Returns:
            Formatted endpoint
        """
        # Replace {param} placeholders in endpoint
        for param_name, param_value in parameters.items():
            placeholder = "{" + param_name + "}"
            if placeholder in endpoint:
                endpoint = endpoint.replace(placeholder, str(param_value))
        
        return endpoint
    
    def _format_value(self, template: str, parameters: Dict[str, Any]) -> Any:
        """
        Format a value template with parameter values.
        
        Args:
            template: Value template with placeholders
            parameters: Parameter values
            
        Returns:
            Formatted value
        """
        # Check if template is a parameter reference
        if isinstance(template, str) and template.startswith("{") and template.endswith("}"):
            param_name = template[1:-1]
            return parameters.get(param_name)
        
        # Not a template, return as is
        return template
    
    def _add_api_key(
        self, 
        api_details: Dict[str, Any], 
        headers: Dict[str, str], 
        params: Dict[str, str]
    ):
        """
        Add API key to headers or parameters as needed.
        
        Args:
            api_details: API details from tool definition
            headers: Headers dictionary to modify
            params: Parameters dictionary to modify
        """
        # Get API provider
        provider = api_details.get("provider", "").lower()
        
        # Check if we have an API key for this provider
        if provider and provider in self.api_keys and self.api_keys[provider]:
            api_key = self.api_keys[provider]
            
            # Check header template for API key placeholder
            for header_name, header_template in api_details.get("headers", {}).items():
                if "{API_KEY}" in header_template:
                    headers[header_name] = header_template.replace("{API_KEY}", api_key)
            
            # Check params template for API key placeholder
            for param_name, param_template in api_details.get("params", {}).items():
                if "{API_KEY}" in param_template:
                    params[param_name] = param_template.replace("{API_KEY}", api_key)
            
            # If no template specifies API key location, use defaults
            if not any("{API_KEY}" in v for v in list(api_details.get("headers", {}).values()) + list(api_details.get("params", {}).values())):
                if provider == "coingecko":
                    params["x_cg_pro_api_key"] = api_key
                elif provider == "etherscan":
                    params["apikey"] = api_key
                elif provider == "infura":
                    # Infura project ID might be in the URL already
                    pass
