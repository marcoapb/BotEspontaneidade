FROM python:3.8-slim-buster

WORKDIR /bot

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
COPY ./requirements.txt /bot
COPY ./Bot_Telegram.py /bot
RUN pip install -r /bot/requirements.txt
RUN mkdir /Log
#COPY . .
CMD ["python", "Bot_Telegram.py"]