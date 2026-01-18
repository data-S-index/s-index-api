#!/bin/bash

# Deploy script for s-index-api
# Assumes the repository is already on the server

set -euo pipefail

# Pull latest changes from repository
echo "Pulling latest changes from git..."
git pull

# Verify Docker and Docker Compose are available
docker version
docker compose version

# Stop and remove existing containers
echo "Stopping and removing existing containers..."
docker compose -f docker-compose-api.yml down --remove-orphans || true

# Remove old/unused Docker images
echo "Removing old Docker images..."
docker image prune -af

# Deploy using api docker-compose file:
# -f: specify compose file
# -d: run in detached mode (background)
# --build: rebuild images before starting
# --remove-orphans: remove containers for services not in compose file
echo "Building and starting containers..."
docker compose -f docker-compose-api.yml up -d --build --remove-orphans

echo "Deployment complete!"
