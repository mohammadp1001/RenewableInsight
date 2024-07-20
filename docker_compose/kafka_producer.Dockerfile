FROM ubuntu:20.04

# Install Python, pip, and netcat
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create a directory called app/data
RUN mkdir -p /app/data


# Copy the requirements file into the container
COPY /docker_compose/requirements.txt .

# Copy producer code from outside the current directory
COPY /src/utilities /app/utilities

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run your Kafka producer script
CMD ["python3", "./utilities/kafka_producer.py"]