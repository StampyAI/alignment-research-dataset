FROM python:3.11-slim-bookworm

COPY align_data /source/align_data
COPY main.py /source/main.py
COPY requirements.txt /source/requirements.txt
WORKDIR /source

RUN apt-get update
RUN apt-get -y install git

RUN useradd --create-home --shell /bin/bash ard
RUN chown ard:ard -R /source
USER ard:ard

RUN python -m pip install --upgrade pip
RUN pip3 install torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install -r requirements.txt

CMD ["python", "main.py", "fetch-all"]