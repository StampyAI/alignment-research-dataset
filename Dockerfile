FROM python:3.11-slim-buster

WORKDIR /app

ADD . /app

RUN apt-get update && apt-get install -y \
    git \
    pkg-config \
    default-libmysqlclient-dev \
    gcc

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "main.py" ]