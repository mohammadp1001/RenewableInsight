FROM mageai/mageai:latest

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
ARG SPARK_PATH=/home/

# Note: this overwrites the requirements.txt file in your new project on first run. 
# You can delete this line for the second run :) 
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt 
COPY setup.sh ${SPARK_PATH}/setup.sh

RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

CMD ["/bin/bash", "-c", "/home/setup.sh"]