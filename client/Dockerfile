
FROM python:latest

ADD client.py /client/

WORKDIR /client/

VOLUME ./output:/output

ENTRYPOINT ["python3", "-u", "client.py"]
