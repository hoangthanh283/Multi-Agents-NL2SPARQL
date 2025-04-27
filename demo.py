#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

import requests


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
        self._port_forward_process = None
    
    def _discover_api_url(self) -> str:
        """
        Attempt to automatically discover the API URL based on Kubernetes configuration.
        
        Returns:
            str: The discovered API URL
        """
        # Method 1: Try port forwarding first (most reliable method)
        try:
            print("Setting up port forwarding to nl2sparql-api service...")
            # Start port forwarding in a new process
            self._port_forward_process = subprocess.Popen(
                ["kubectl", "port-forward", "service/nl2sparql-api", "8001:8001"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give it a moment to establish
            time.sleep(3)
            
            # Check if the process is still running
            if self._port_forward_process.poll() is None:
                print("Port forwarding successfully established")
                return "http://localhost:8001"
            else:
                # Read error output
                _, stderr = self._port_forward_process.communicate()
                print(f"Port forwarding failed: {stderr}")
        except Exception as e:
            print(f"Could not set up port forwarding: {e}")
        
        # Method 2: Try to get Minikube IP and NodePort if port forwarding failed
        try:
            minikube_ip = self._get_minikube_ip()
            node_port = self._get_nodeport()
            
            if minikube_ip and node_port:
                print(f"Using Minikube IP ({minikube_ip}) and NodePort ({node_port})")
                return f"http://{minikube_ip}:{node_port}"
        except Exception as e:
            print(f"Could not discover using Minikube IP and NodePort: {e}")
        
        # Method 3: Check if we should use the Ingress URL
        try:
            ingress_host = self._get_ingress_host()
            if ingress_host:
                print(f"Using Ingress host: {ingress_host}")
                return f"http://{ingress_host}"
        except Exception as e:
            print(f"Could not discover using Ingress: {e}")
        
        # Default fallback
        print("Using default localhost:8001 endpoint")
        return "http://localhost:8001"
    
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
    
    def _get_ingress_host(self) -> Optional[str]:
        """Get the hostname for the nl2sparql API ingress"""
        try:
            result = subprocess.run(
                ["kubectl", "get", "ingress", "nl2sparql-ingress", "-o", "jsonpath={.spec.rules[0].host}"],
                capture_output=True,
                text=True
            )
            host = result.stdout.strip()
            if host:
                # Check if this hostname is in /etc/hosts
                with open('/etc/hosts', 'r') as f:
                    if host in f.read():
                        return host
            return None
        except Exception:
            return None
            
    def __del__(self):
        """Clean up port forwarding process when the client is deleted"""
        if self._port_forward_process is not None:
            try:
                self._port_forward_process.terminate()
                print("Port forwarding terminated")
            except Exception:
                pass
    
    def submit_query(self, query: str, context: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Submit a natural language query to the NL2SPARQL API.
        Try both POST (as designed) and GET (as apparently required by the server).
        
        Args:
            query: The natural language query
            context: Optional context information to help with query processing
            
        Returns:
            Dict containing the task_id
        """
        endpoint = f"{self.base_url}/api/nl2sparql"
        payload = {"query": query, "context": context or []}
        
        # First try POST as per the API design
        print(f"Submitting query to {endpoint}...")
        try:
            response = requests.post(endpoint, json=payload, timeout=self.timeout)
            if response.status_code == 200:
                print("POST request successful")
                return response.json()
            elif response.status_code == 405:  # Method Not Allowed
                print("POST method not allowed, trying GET...")
            else:
                raise Exception(f"Error submitting query: {response.status_code} - {response.text}")
        except requests.exceptions.ConnectionError as e:
            if "Connection refused" in str(e) and self.base_url != "http://localhost:8001":
                print("Connection refused. Trying fallback to port forwarding...")
                self.base_url = "http://localhost:8001"
                # Start port forwarding if not already running
                if self._port_forward_process is None:
                    self._port_forward_process = subprocess.Popen(
                        ["kubectl", "port-forward", "service/nl2sparql-api", "8001:8001"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    time.sleep(3)
                # Try again with the new base URL
                return self.submit_query(query, context)
            raise
        
        # If POST fails with 405, try GET instead
        try:
            # For GET request, we need to pass the query as a parameter
            params = {"query": query}
            if context:
                # Since GET params are strings, we need to serialize complex objects
                params["context"] = json.dumps(context)
                
            response = requests.get(endpoint, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                print("GET request successful")
                return response.json()
            else:
                raise Exception(f"Error submitting GET query: {response.status_code} - {response.text}")
        except Exception as e:
            raise Exception(f"All request methods failed: {str(e)}")
    
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
                
                print(f"Query still processing... (elapsed: {elapsed:.1f}s)")
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
        sys.exit(1)

if __name__ == "__main__":
    main()
