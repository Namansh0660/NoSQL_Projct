#!/bin/bash

# NoSQL Knowledge Graph Project Startup Script
# This script starts the complete pipeline including Kafka, MongoDB, and the Streamlit dashboard

echo "🚀 Starting NoSQL Knowledge Graph Project..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found. Creating from template..."
    cp .env.example .env
    echo "📝 Please edit .env file with your MongoDB Atlas credentials before running again."
    echo "   For local development, you can use the default MongoDB container settings."
    exit 1
fi

# Function to wait for service to be ready
wait_for_service() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $service_name | grep -q "Up (healthy)"; then
            echo "✅ $service_name is ready!"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service_name failed to start within expected time"
    return 1
}

# Build and start services
echo "🏗️  Building Docker images..."
docker-compose build

echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for core services
wait_for_service kafka
wait_for_service mongo

echo ""
echo "🎉 NoSQL Knowledge Graph Project is starting up!"
echo ""
echo "📊 Access points:"
echo "   • Streamlit Dashboard: http://localhost:8501"
echo "   • Kafka UI: http://localhost:8080"
echo "   • MongoDB Express: http://localhost:8081"
echo "   • MongoDB: localhost:27017"
echo "   • Kafka: localhost:9092"
echo ""
echo "📝 Services starting:"
echo "   • Kafka Producer (fetching papers from APIs)"
echo "   • Kafka Consumer (building knowledge graph)"
echo "   • Streamlit Dashboard (real-time visualization)"
echo ""
echo "🔍 To view logs:"
echo "   docker-compose logs -f [service-name]"
echo ""
echo "🛑 To stop all services:"
echo "   docker-compose down"
echo ""
echo "⏳ Please wait a moment for all services to fully initialize..."

# Follow logs from main application
sleep 5
echo "📊 Starting to show application logs..."
docker-compose logs -f nosql-app