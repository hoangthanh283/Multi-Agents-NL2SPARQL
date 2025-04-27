import requests

# Use port forwarding (kubectl port-forward service/nl2sparql-api 8000:8000)
response = requests.post(
    "http://localhost:8000/api/nl2sparql",
    json={"query": "Get all classes in the Knowledge Graph", "context": []}
)
print(response.json())
