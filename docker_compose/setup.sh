#!/bin/bash

# Define the directory name
DIR_NAME="/home/spark"

# Create the directory
mkdir -p "$DIR_NAME"

# Change to the directory
cd "$DIR_NAME"

# Download the file
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz

# Extract the file
tar -xzf openjdk-11.0.2_linux-x64_bin.tar.gz

# Set the environment variables
export JAVA_HOME="$(pwd)/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

# Print the environment variables to verify
echo "JAVA_HOME is set to $JAVA_HOME"
echo "PATH is set to $PATH"
