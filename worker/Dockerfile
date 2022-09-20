FROM python:latest

ADD worker.py /worker/
ADD test.txt /worker/
ADD longggg.txt /worker/

WORKDIR /worker/

ENTRYPOINT ["python3", "-u", "worker.py"]
