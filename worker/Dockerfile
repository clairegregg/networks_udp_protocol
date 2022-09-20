FROM python:latest

ADD worker.py /worker/

WORKDIR /worker/

ENTRYPOINT ["python3", "-u", "worker.py"]
