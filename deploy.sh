#!/bin/bash
# Deployment script for Multi-Agents-NL2SPARQL
# This script validates code and deploys it to Kubernetes

set -e  # Exit on error

# Display banner
echo "========================================="
echo "  NL2SPARQL Kubernetes Deployment Tool"
echo "========================================="
echo

# Functions
function check_command() {
  if ! command -v $1 &> /dev/null; then
    echo "Error: $1 is not installed. Please install it first."
    return 1
  fi
  return 0
}

function install_docker() {
  echo "Installing Docker on Ubuntu 20.04..."
  sudo apt-get update
  sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io
  
  # Add user to docker group
  sudo usermod -aG docker $USER
  echo "Docker installed. You may need to log out and back in for group changes to take effect."
  
  # Start Docker service
  sudo systemctl enable docker
  sudo systemctl start docker
}

function install_kubectl() {
  echo "Installing kubectl on Ubuntu 20.04..."
  sudo apt-get update
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg https://packages.cloud.google.com/apt/doc/apt-key.gpg
  echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee /etc/apt/sources.list.d/kubernetes.list
  sudo apt-get update
  sudo apt-get install -y kubectl
  kubectl version --client
}

function install_minikube() {
  echo "Installing Minikube on Ubuntu 20.04..."
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  sudo install minikube-linux-amd64 /usr/local/bin/minikube
  rm minikube-linux-amd64
  minikube version
}

function install_helm() {
  echo "Installing Helm on Ubuntu 20.04..."
  curl https://baltocdn.com/helm/signing.asc | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
  sudo apt-get install apt-transport-https --yes
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
  sudo apt-get update
  sudo apt-get install helm
}

function setup_prerequisites() {
  echo "Setting up prerequisites for Ubuntu 20.04..."
  
  # Check and install Docker if needed
  if ! check_command docker; then
    read -p "Docker is not installed. Would you like to install it? (y/N): " INSTALL_DOCKER
    if [[ "$INSTALL_DOCKER" =~ ^[Yy]$ ]]; then
      install_docker
    else
      echo "Docker is required. Please install it manually and try again."
      exit 1
    fi
  else
    echo "✓ Docker is installed"
  fi
  
  # Check Docker service status
  if ! sudo systemctl is-active --quiet docker; then
    echo "Docker service is not running. Starting it..."
    sudo systemctl start docker
  fi
  
  # Check and install kubectl if needed
  if ! check_command kubectl; then
    read -p "kubectl is not installed. Would you like to install it? (y/N): " INSTALL_KUBECTL
    if [[ "$INSTALL_KUBECTL" =~ ^[Yy]$ ]]; then
      install_kubectl
    else
      echo "kubectl is required. Please install it manually and try again."
      exit 1
    fi
  else
    echo "✓ kubectl is installed"
  fi
  
  # Check and install Minikube if needed
  if ! check_command minikube; then
    read -p "Minikube is not installed. Would you like to install it? (y/N): " INSTALL_MINIKUBE
    if [[ "$INSTALL_MINIKUBE" =~ ^[Yy]$ ]]; then
      install_minikube
    else
      echo "A Kubernetes cluster is required. Please set up a cluster manually and try again."
      exit 1
    fi
  else
    echo "✓ Minikube is installed"
    
    # Check if minikube is running
    minikube_status=$(minikube status -f '{{.Host}}' 2>/dev/null || echo "Not Running")
    if [ "$minikube_status" != "Running" ]; then
      echo "Minikube is not running. Starting minikube..."
      minikube start --driver=docker --cpus=2 --memory=4g
    fi
  fi
  
  # Check and install Helm if needed (optional)
  if ! check_command helm; then
    read -p "Helm is not installed. Would you like to install it? (y/N): " INSTALL_HELM
    if [[ "$INSTALL_HELM" =~ ^[Yy]$ ]]; then
      install_helm
      echo "✓ Helm is installed"
    else
      echo "Helm installation skipped. This is optional."
    fi
  else
    echo "✓ Helm is installed"
  fi
  
  echo "✓ All prerequisites are set up"
}

function wait_for_pods() {
  echo "Waiting for pods to be ready..."
  kubectl wait --for=condition=ready pods --all --timeout=300s
  if [ $? -ne 0 ]; then
    echo "Warning: Not all pods are ready. Check their status with 'kubectl get pods'."
  fi
}

# Step 0: Check and setup prerequisites
echo "Step 0: Setting up prerequisites..."
setup_prerequisites
echo

# Step 1: Validate code
echo "Step 1: Running code validation..."
./validate.py
if [ $? -ne 0 ]; then
    echo "❌ Validation failed! Fix the errors before deploying."
    exit 1
fi
echo "✓ Validation successful!"
echo

# Step 2: Build Docker images
echo "Step 2: Building Docker images..."
if command -v minikube &> /dev/null; then
  eval $(minikube docker-env)
fi

docker build -t nl2sparql-api:latest -f Dockerfile .
docker build -t nl2sparql-worker:latest -f Dockerfile.worker .
echo "✓ Docker images built successfully!"
echo

# Ask if user wants to push images to a registry
read -p "Do you want to push images to a registry? (y/N): " PUSH_IMAGES
if [[ "$PUSH_IMAGES" =~ ^[Yy]$ ]]; then
  read -p "Enter your registry (e.g., docker.io/username): " REGISTRY
  echo "Tagging and pushing images to $REGISTRY..."
  docker tag nl2sparql-api:latest $REGISTRY/nl2sparql-api:latest
  docker tag nl2sparql-worker:latest $REGISTRY/nl2sparql-worker:latest
  docker push $REGISTRY/nl2sparql-api:latest
  docker push $REGISTRY/nl2sparql-worker:latest
  echo "✓ Images pushed to registry"
fi
echo

# Step 3: Deploy Dependencies
echo "Step 3: Deploying dependencies..."
echo "3.1: Deploying Redis..."
# Create ConfigMap for redis.conf
echo "Creating ConfigMap for Redis configuration..."
kubectl create configmap redis-config --from-file=redis.conf -o yaml --dry-run=client | kubectl apply -f -
kubectl apply -f k8s/redis-statefulset.yml
kubectl apply -f k8s/redis-service.yml

echo "3.2: Deploying GraphDB..."
# Create ConfigMap for GraphDB cluster configuration
echo "Creating ConfigMap for GraphDB cluster configuration..."
kubectl create configmap graphdb-cluster-config --from-file=cluster-config.ttl -o yaml --dry-run=client | kubectl apply -f -
kubectl apply -f k8s/graphdb-statefulset.yml
kubectl apply -f k8s/graphdb-service.yml

echo "3.3: Deploying Qdrant vector database..."
kubectl apply -f k8s/qdrant-statefulset.yml
kubectl apply -f k8s/qdrant-service.yml

echo "3.4: Deploying Kafka and Zookeeper..."
kubectl apply -f k8s/kafka-deployment.yml

echo "3.5: Deploying Dask cluster..."
kubectl apply -f k8s/dask-deployment.yml

echo "3.6: Deploying Ray cluster..."
kubectl apply -f k8s/ray-deployment.yml

echo "✓ Dependencies deployed successfully"
echo

# Wait for dependencies to be ready
echo "Waiting for dependencies to be ready before deploying core services..."
sleep 30
wait_for_pods
echo

# Step 4: Deploy Core Services
echo "Step 4: Deploying core services..."
echo "4.1: Deploying API (Global Master)..."
kubectl apply -f k8s/api-deployment.yml
kubectl apply -f k8s/api-service.yml

echo "4.2: Deploying Domain Masters..."
kubectl apply -f k8s/domain-masters-deployment.yml

echo "4.3: Deploying Workers..."
kubectl apply -f k8s/worker-deployment.yml

echo "✓ Core services deployed successfully"
echo

# Step 5: Deploy Monitoring (if configurations exist)
echo "Step 5: Deploying monitoring services..."
if [ -f "k8s/prometheus-config.yml" ]; then
  echo "5.1: Deploying Prometheus..."
  kubectl apply -f k8s/prometheus-config.yml
fi

if [ -f "k8s/grafana-config.yml" ]; then
  echo "5.2: Deploying Grafana..."
  kubectl apply -f k8s/grafana-config.yml
fi

echo "✓ Monitoring services deployed (if configured)"
echo

# Step 6: Deploy Ingress/Load Balancer (if configured)
echo "Step 6: Configuring external access..."
if [ -f "k8s/ingress.yml" ]; then
  echo "6.1: Deploying Ingress..."
  kubectl apply -f k8s/ingress.yml
fi

if [ -f "k8s/istio-config.yml" ]; then
  echo "6.2: Deploying Istio config..."
  kubectl apply -f k8s/istio-config.yml
fi

echo "✓ External access configured (if applicable)"
echo

# Step 7: Verify Deployment
echo "Step 7: Verifying deployment..."
echo "7.1: Checking pods status..."
kubectl get pods

echo "7.2: Checking services..."
kubectl get svc

echo "7.3: Checking API service specifically..."
API_POD=$(kubectl get pods -l app=nl2sparql-api -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || echo "not-found")
if [ "$API_POD" != "not-found" ]; then
  echo "API pod found: $API_POD"
  echo "API pod logs:"
  kubectl logs $API_POD --tail=20
else
  echo "Warning: API pod not found. Check deployment."
fi
echo

# Step 8: Test the API
echo "Step 8: Testing the API..."
read -p "Do you want to port-forward the API service for testing? (Y/n): " TEST_API
if [[ ! "$TEST_API" =~ ^[Nn]$ ]]; then
  echo "Starting port forwarding in the background..."
  kubectl port-forward svc/nl2sparql-api 8000:8000 &
  PORT_FORWARD_PID=$!
  
  # Give it time to establish the connection
  sleep 5
  
  echo "Sending test request to API..."
  curl -X POST "http://localhost:8000/api/nl2sparql" \
    -H "Content-Type: application/json" \
    -d '{"query": "What are the symptoms of COVID-19?", "context": []}' \
    -w "\n"
  
  echo "Checking master status..."
  curl "http://localhost:8000/api/master/status" -w "\n"
  
  # Kill port-forwarding
  kill $PORT_FORWARD_PID >/dev/null 2>&1
fi
echo

# Step 9: Monitoring Access
echo "Step 9: Monitoring access..."
echo "To access monitoring dashboards, run:"
echo "  kubectl port-forward svc/prometheus 9090:9090"
echo "  kubectl port-forward svc/grafana 3000:3000"
echo

# Step 10: Provide cleanup instructions
echo "Step 10: Cleanup instructions..."
echo "To remove all deployed resources, run:"
echo "  kubectl delete -f k8s/"
echo

echo "========================================="
echo "✅ Deployment completed!"
echo "Run 'kubectl get pods' to check the status of your pods"
echo "Run 'kubectl port-forward svc/nl2sparql-api 8000:8000' to access the API"
echo "========================================="