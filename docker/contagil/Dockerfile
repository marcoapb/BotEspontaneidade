FROM python:3.8.3-slim-buster

WORKDIR /bot

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get -y install netcat gcc \
    && apt-get clean

#ADD . /bot

RUN pip install --upgrade pip
COPY ./reqServContagil.txt /bot
COPY ./ServidorSQLContagil.py /bot
#COPY ./mykeyPrivada.pem /bot
RUN pip install -r /bot/reqServContagil.txt
RUN mkdir /Log
#COPY . .
CMD ["python", "ServidorSQLContagil.py"]