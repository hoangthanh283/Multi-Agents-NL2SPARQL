#!/usr/bin/env python3
import requests
import subprocess
import json
import time
import argparse
from typing import Dict, Any, Optional, List

class NL2SPARQLClient:
    """Client for interacting with the NL2SPARQL API in Kubernetes"""
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize the NL2SPARQL client.
        
        Args:
            base_url: Base URL for the API. If None, will attempt to discover the URL.
        """
        self.base_url = base_url or self._discover_api_url()
        self.timeout = 10  # Default timeout for requests in seconds
    
    def _discover_api_url(self) -> str:
        """
        Attempt to automatically discover the API URL based on Kubernetes configuration.
        
        Returns:
            str: The discovered API URL
        """
        # Method 1: Try to get Minikube IP and NodePort
        try:
            minikube_ip = self._get_minikube_ip()
            node_port = self._get_nodeport()
            
            if minikube_ip and node_port:
                return f"http://{minikube_ip}:{node_port}"
        except Exception as e:
            print(f"Could not discover using Minikube IP and NodePort: {e}")
        
        # Method 2: Try port forwarding if method 1 fails
        try:
            # Check if port forwarding is already active on port 8000
            result = subprocess.run(
                ["lsof", "-i", ":8000"], 
                capture_output=True, 
                text=True
            )
            
            if "kubectl" not in result.stdout:
                print("Setting up port forwarding to nl2sparql-api service...")
                # Start port forwarding in the background
                subprocess.Popen(
                    ["kubectl", "port-forward", "service/nl2sparql-api", "8000:8000"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                # Give it a moment to establish
                time.sleep(2)
            
            return "http://localhost:8000"
        except Exception as e:
            print(f"Could not set up port forwarding: {e}")
        
        # Default fallback
        return "http://localhost:8000"
    
    def _get_minikube_ip(self) -> Optional[str]:
        """Get the Minikube IP address using the minikube command"""
        try:
            result = subprocess.run(
                ["minikube", "ip"], 
                capture_output=True, 
                text=True, 
                check=True
            )
            return result.stdout.strip()
        except Exception:
            return None
    
    def _get_nodeport(self) -> Optional[str]:
        """Get the NodePort for the nl2sparql-api service"""
        try:
            result = subprocess.run(
                ["kubectl", "get", "service", "nl2sparql-api", "-o", "jsonpath={.spec.ports[0].nodePort}"], 
                capture_output=True, 
                text=True,
                check=True
            )
            return result.stdout.strip()
        except Exception:
            return None
    
    def submit_query(self, query: str, context: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Submit a natural language query to the NL2SPARQL API.
        
        Args:
            query: The natural language query
            context: Optional context information to help with query processing
            
        Returns:
            Dict containing the task_id
        """
        endpoint = f"{self.base_url}/api/nl2sparql"
        payload = {"query": query, "context": context or []}
        
        print(f"Submitting query to {endpoint}...")
        response = requests.post(endpoint, json=payload, timeout=self.timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error submitting query: {response.status_code} - {response.text}")
    
    def get_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get the status of a submitted query.
        
        Args:
            task_id: The task ID returned from submit_query
            
        Returns:
            Dict containing the task status
        """
        endpoint = f"{self.base_url}/api/nl2sparql/{task_id}/status"
        
        response = requests.get(endpoint, timeout=self.timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error getting status: {response.status_code} - {response.text}")
    
    def get_result(self, task_id: str) -> Dict[str, Any]:
        """
        Get the result of a completed query.
        
        Args:
            task_id: The task ID returned from submit_query
            
        Returns:
            Dict containing the query result
        """
        endpoint = f"{self.base_url}/api/nl2sparql/{task_id}/result"
        
        response = requests.get(endpoint, timeout=self.timeout)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error getting result: {response.status_code} - {response.text}")
    
    def wait_for_result(self, task_id: str, max_wait: int = 60, poll_interval: int = 2) -> Dict[str, Any]:
        """
        Wait for a query to complete and return the result.
        
        Args:
            task_id: The task ID returned from submit_query
            max_wait: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            Dict containing the query result
        """
        start_time = time.time()
        elapsed = 0
        
        while elapsed < max_wait:
            try:
                result = self.get_result(task_id)
                
                # If the result is no longer pending, return it
                if "status" not in result or result["status"] != "pending":
                    return result
                
                print(f"Query still processing... (elapsed: {elapsed}s)")
                time.sleep(poll_interval)
                elapsed = time.time() - start_time
            except Exception as e:
                print(f"Error checking result: {e}")
                time.sleep(poll_interval)
                elapsed = time.time() - start_time
        
        raise Exception(f"Timeout waiting for query result after {max_wait} seconds")

def format_result(result: Dict[str, Any]) -> str:
    """
    Format the query result for display.
    
    Args:
        result: The query result from the API
        
    Returns:
        Formatted string representation of the result
    """
    output = []
    
    # Add basic info
    output.append("=" * 80)
    output.append("NL2SPARQL QUERY RESULT")
    output.append("=" * 80)
    
    # Check if result contains the full workflow data
    if "data" in result:
        data = result["data"]
        
        # Show original query
        if "query" in data:
            output.append(f"Natural Language Query: {data['query']}")
        
        # Show if result came from cache
        if "from_cache" in data and data["from_cache"]:
            output.append(f"Result from cache: Yes (type: {data.get('cache_type', 'unknown')})")
        
        # Show generated SPARQL
        if "sparql_query" in data:
            output.append("\nGenerated SPARQL Query:")
            output.append("-" * 80)
            output.append(data["sparql_query"])
            output.append("-" * 80)
        
        # Show response
        if "response" in data:
            output.append("\nResponse:")
            output.append("-" * 80)
            output.append(data["response"])
            output.append("-" * 80)
    
    # Show workflow steps if available
    if "steps" in result:
        output.append("\nWorkflow Steps:")
        for step in result["steps"]:
            domain = step.get("domain", "unknown")
            status = step.get("status", "unknown")
            
            start_time = step.get("start_time")
            end_time = step.get("end_time")
            
            if start_time and end_time:
                duration = round(end_time - start_time, 2)
                output.append(f"- {domain}: {status} (took {duration}s)")
            else:
                output.append(f"- {domain}: {status}")
    
    # Show execution time
    if "created_at" in result and "completed_at" in result:
        total_time = round(result["completed_at"] - result["created_at"], 2)
        output.append(f"\nTotal execution time: {total_time}s")
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description="NL2SPARQL Demo Client")
    parser.add_argument("--query", type=str, help="The natural language query to process")
    parser.add_argument("--api-url", type=str, help="Base URL for the API (default: auto-discover)")
    parser.add_argument("--timeout", type=int, default=60, help="Maximum time to wait for result in seconds")
    args = parser.parse_args()
    
    # Use the default query if none provided
    query = args.query or "Get all classes in the Knowledge Graph"
    
    try:
        # Initialize the client
        client = NL2SPARQLClient(args.api_url)
        
        print(f"Using API endpoint: {client.base_url}")
        print(f"Query: {query}")
        
        # Submit the query
        response = client.submit_query(query)
        task_id = response.get("task_id")
        
        if not task_id:
            print("Error: No task_id returned from API")
            return
            
        print(f"Query submitted successfully. Task ID: {task_id}")
        
        # Wait for the result
        print(f"Waiting for result (timeout: {args.timeout}s)...")
        result = client.wait_for_result(task_id, max_wait=args.timeout)
        
        # Format and display the result
        formatted_result = format_result(result)
        print(formatted_result)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
