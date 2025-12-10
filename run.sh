#!/bin/bash

# NoSQL Pasture Management - Quick Run Script

set -e  # Exit on error

echo "=========================================="
echo "NoSQL Pasture Management System"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Start databases
echo "🚀 Step 1: Starting databases..."
docker-compose up -d

echo "⏳ Waiting for databases to initialize (5 seconds)..."
sleep 5

echo ""
echo "✅ Databases are ready!"
echo ""

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "📦 Step 2: Creating virtual environment..."
    python3 -m venv venv
fi

# Activate venv and install dependencies
echo "📦 Installing Python dependencies..."
source venv/bin/activate
pip install -q -r requirements.txt

echo ""
echo "🔄 Step 3: Running data ingestion pipeline..."
echo ""
cd src/ingestion
python pipeline.py

echo ""
echo "📊 Step 4: Running analytics queries..."
echo ""
cd ../queries
python analytics_queries.py

echo ""
echo "=========================================="
echo "✅ ALL DONE!"
echo "=========================================="
echo ""
echo "Database access:"
echo "  - MongoDB:   mongodb://admin:password@localhost:27017/"
echo "  - Cassandra: cqlsh localhost 9042"
echo "  - Redis:     redis-cli"
echo "  - Neo4j:     http://localhost:7474 (neo4j/password)"
echo ""
echo "To stop databases: docker-compose down"
echo ""
