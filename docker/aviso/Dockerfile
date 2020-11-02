FROM python:3.8.3-slim-buster

WORKDIR /aviso

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
#ENV MYSQL_ROOT_PASSWORD "MySql2020"
#ENV MYSQL_DATABASE "testedb"
#ENV MYSQL_USER "my_user"
#ENV MYSQL_PASSWORD "mypass1234"

RUN apt-get update \
    && apt-get -y install netcat gcc \
    && apt-get clean

#ADD . /bot

RUN pip install --upgrade pip
COPY ./reqAviso.txt /aviso
COPY ./AvisoUrgente.py /aviso
RUN pip install -r /aviso/reqAviso.txt
RUN mkdir /Log
#COPY . .
CMD ["python", "AvisoUrgente.py"]