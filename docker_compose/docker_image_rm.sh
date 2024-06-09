#!/bin/bash

# Check if the number of rows to remove is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_rows_to_remove>"
  exit 1
fi

# Number of rows to remove
M=$1

# Extract the IMAGE IDs from the file
IMAGE_IDS=$(tail -n +2 docker_image_list.txt | head -n $M | awk '{print $3}')

# Check if IMAGE_IDS is empty
if [ -z "$IMAGE_IDS" ]; then
  echo "No IMAGE IDs found to remove."
  exit 1
fi

# Run the docker rmi command with the IMAGE IDs
docker image rmi -f $IMAGE_IDS
