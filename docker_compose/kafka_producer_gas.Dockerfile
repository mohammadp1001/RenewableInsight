FROM ubuntu:20.04

# Install Python, pip, and netcat
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Change working directory to /app
WORKDIR /app

# Copy the requirements file into the container
COPY /docker_compose/requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the command to run your Kafka producer script
CMD ["python3", "/app/RenewableInsight/src/kafka_producer_gas.py"]
