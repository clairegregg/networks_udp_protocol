
FROM python:latest

ADD client/client.py /client/
COPY protocol_lib.py /client/

WORKDIR /client/

VOLUME ./output:/output

ENTRYPOINT ["python3", "-u", "client.py"]
