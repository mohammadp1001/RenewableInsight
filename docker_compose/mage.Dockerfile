FROM mageai/mageai:latest


# Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# Install OpenJDK 11 and git
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk 

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list

# Note: this overwrites the requirements.txt file in your new project on first run. 
# You can delete this line for the second run :) 
COPY ./docker_compose/requirements.txt .

RUN pip install -r requirements.txt