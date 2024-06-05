# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire src directory into the container
COPY ../src/utilities ./src

# Set the working directory to src/utilities
WORKDIR /app/src/utilities

# Run the Python script on container start
CMD ["python", "kafka_producer.py"]
