FROM python:3.8.3-slim-buster

WORKDIR /botImportador

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get -y install netcat gcc \
    && apt-get clean

#ADD . /bot

RUN pip install --upgrade pip
COPY ./reqImportador.txt /botImportador
COPY ./ImportaArquivos.py /botImportador
RUN pip install -r /botImportador/reqImportador.txt
RUN mkdir /Excel
#COPY . .
CMD ["python", "ImportaArquivos.py"]