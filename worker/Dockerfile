FROM python:latest

ADD worker.py /worker/
ADD test.txt /worker/
ADD long_test.txt /worker/
ADD longer_than_buffer_test.txt /worker/

WORKDIR /worker/

ENTRYPOINT ["python3", "-u", "worker.py"]
