FROM python:3.8.3-slim-buster

WORKDIR /botCarga

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get -y install netcat gcc \
    && apt-get clean

#ADD . /botCarga

RUN pip install --upgrade pip
COPY ./reqCarga.txt /botCarga
COPY ./CargaPlanilhasExcel.py /botCarga
RUN pip install -r /botCarga/reqCarga.txt
RUN mkdir /Log
RUN mkdir /Excel
#COPY ./*.xlsx /botCarga/
CMD ["python", "CargaPlanilhasExcel.py"]