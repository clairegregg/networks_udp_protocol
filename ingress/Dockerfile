# Author: Claire Gregg

FROM python:latest

ADD ingress/ingress.py /ingress/
COPY protocol_lib.py /ingress/

WORKDIR /ingress/

# THIS IS ESSENTIAL FOR THE CLIENT SERVER COMMUNICATION
EXPOSE 20001/udp

ENTRYPOINT ["python3", "-u", "ingress.py"]
