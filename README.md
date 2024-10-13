# RenewableInsight
Process, and analyze renewable energy data.




Create a docker network:

- docker network create prefect-network

First build the kafka services and producers.
Then build prefect server and agent.

- docker-compose up -d kafka
- docker-compose up -d producer
- docker-compose up -d server
- docker-compose up -d agent
- docker-compose up -d orchestrator


secret manager