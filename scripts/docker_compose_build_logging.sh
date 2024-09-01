#!/bin/bash

LOGFILE="compose_build_$(date +%Y%m%d_%H%M%S).log"

echo "Starting Docker Compose build..." | tee -a $LOGFILE
docker-compose build 2>&1 | ts '[%Y-%m-%d %H:%M:%S]' | tee -a $LOGFILE

if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "Docker Compose build failed!" | tee -a $LOGFILE
  exit 1
else
  echo "Docker Compose build completed successfully." | tee -a $LOGFILE
fi
