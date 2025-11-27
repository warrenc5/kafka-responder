FROM python:latest
RUN apt-get update
RUN apt-get -y install python3 python3-pip python3-avro python3-kafka python3-yaml 
RUN apt-get -y install strace
RUN python3 -m pip install avro kafka
ADD responder.py /responder/
WORKDIR /responder
