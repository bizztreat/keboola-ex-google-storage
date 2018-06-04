FROM quay.io/keboola/docker-custom-python:latest

RUN apt-get update && apt-get install -y python3-pip
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --upgrade oauth2client
RUN python3 -m pip install --upgrade google-api-python-client
COPY . /code/
#COPY data/ /data/
WORKDIR /data/
CMD ["python3", "-u", "/code/main.py"]
