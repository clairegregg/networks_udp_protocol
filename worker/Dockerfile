# Author: Claire Gregg

FROM python:latest

ADD worker/worker.py /worker/
ADD worker/test_files/ /worker/
COPY protocol_lib.py /worker/

WORKDIR /worker/

ENTRYPOINT ["python3", "-u", "worker.py"]
