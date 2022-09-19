FROM python:latest

ADD worker.py /worker/

WORKDIR /worker/

# THIS IS ESSENTIAL FOR THE CLIENT SERVER COMMUNICATION
EXPOSE 30001/udp

ENTRYPOINT ["python3", "-u", "worker.py"]
