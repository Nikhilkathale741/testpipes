#!/bin/bash

# RDS to On-Premise Database Migration Setup Script

set -e

echo "🚀 Setting up RDS to On-Premise Database Migration"

# Create necessary directories
mkdir -p keys
mkdir -p logs

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📝 Creating .env file from example..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your actual configuration before running migration"
    exit 1
fi

# Check if SSH key exists
SSH_KEY_PATH=$(grep SSH_KEY_PATH .env | cut -d '=' -f2)
if [ ! -f "./keys/$(basename $SSH_KEY_PATH)" ]; then
    echo "⚠️  Please copy your SSH key to ./keys/ directory"
    echo "   Expected: ./keys/$(basename $SSH_KEY_PATH)"
    exit 1
fi

# Set correct permissions for SSH key
chmod 600 ./keys/*

echo "🔨 Building Docker image..."
docker-compose build

echo "✅ Setup complete!"
echo ""
echo "To run the migration:"
echo "  docker-compose up"
echo ""
echo "To run in background:"
echo "  docker-compose up -d"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"