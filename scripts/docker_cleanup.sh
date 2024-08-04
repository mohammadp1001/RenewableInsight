#!/bin/bash

# Stop all running Docker containers
echo "Stopping all running Docker containers..."
docker stop $(docker ps -q)
echo "All running Docker containers have been stopped."

# Prune all unused Docker images
echo "Pruning all unused Docker images..."
docker image prune -a -f
echo "All unused Docker images have been pruned."

# Prune all unused Docker volumes
echo "Pruning all unused Docker volumes..."
docker volume prune -f
echo "All unused Docker volumes have been pruned."

# Prune all unused build cache
echo "Pruning all unused build cache..."
docker buildx prune -f
echo "All unused build cache have been pruned."

# Remove all Docker images
echo "Removing all Docker images..."
docker rmi $(docker images -q) -f
echo "All Docker images have been removed."

# Prune all stopped Docker containers
echo "Pruning all stopped Docker containers..."
docker container prune -f
echo "All stopped Docker containers have been pruned."

echo "Docker cleanup complete."
