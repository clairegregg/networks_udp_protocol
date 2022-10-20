
FROM python:latest

ADD interactive_client/interactive_client.py /interactive_client/
COPY protocol_lib.py /interactive_client/

WORKDIR /interactive_client/

VOLUME ./interactive_output:/interactive_output

ENTRYPOINT ["python3", "-u", "interactive_client.py"]
