"""
Created on Thu Jul 16 11:21:16 2020

@author: 53363833172
"""

from __future__ import unicode_literals
from datetime import datetime
import time
import sys
import os
import logging   
import mysql.connector
from mysql.connector import errorcode
from telegram.ext import Updater
from telegram.ext import CommandHandler, CallbackQueryHandler, Filters, MessageHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton    
############################### Bot ############################################

def disparaAviso():
    global updater, conn
    logging.info("Acionado o disparo de mensagens URGENTES - "+datetime.now().strftime('%d/%m/%Y %H:%M'))
    cursor = conn.cursor()
    dataAtual = datetime.today().date()
    cursor.execute('Select Mensagem from AvisosUrgentes Where DataEnvio Is Null')    
    mensagens = cursor.fetchall()
    msgCofis = ""
    for mensagem in mensagens:
        if msgCofis!="":
            msgCofis = msgCofis+";\n"
        msgCofis = msgCofis+mensagem[0]
    if msgCofis!="":
        msgCofis = "Mensagem URGENTE Cofis:\n"+msgCofis+"."  
    else:
        logging.info("Não há mensagem a ser enviada")      
        return
    comando = "Select idTelegram, CPF from Usuarios Where Saida Is Null"
    cursor.execute(comando)
    usuarios = cursor.fetchall()
    totalMsg = 0
    msgDisparadas = 0
    for usuario in usuarios: #percorremos os usuários ativos Telegram
        logging.info("Disparando para "+usuario[1])
        updater.bot.send_message(usuario[0], text=msgCofis)   
        totalMsg+=1
        msgDisparadas+=1
        if msgDisparadas>=30:
            msgDisparadas = 0
            time.sleep(1) #a cada 30 mensagens, dormimos um segundo (limitação do Bot é 30 por seg - TESTE) 

    try:
        comando = "Update AvisosUrgentes Set DataEnvio=%s Where DataEnvio Is Null"
        cursor.execute(comando, (dataAtual,))
        conn.commit()
    except:
        logging.info("Erro ao atualizar a tabela de AvisosUrgentes - datas de envio ficaram em branco. Cuidado para não reenviar.")
    logging.info("Total de mensagens disparadas: "+str(totalMsg)) 
    return


sistema = sys.platform.upper()
if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
    hostSrv = 'localhost'
    dirLog = 'log\\'
else:
    hostSrv = 'mysqlsrv'
    dirLog = '/Log/'  

#print(datetime.now().strftime('%Y-%m-%d %H_%M')+' BotLog'+sistema+'.log')
    
logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Aviso'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO)

MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "testedb")
MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234")
token = os.getenv("TOKEN", "ERRO")

if token=="ERRO":
    logging.error("Token do Bot Telegram não foi fornecido.")
    print("Token não informado")
    sys.exit(1)

try:
    logging.info("Conectando ao servidor de banco de dados ...")
    logging.info(MYSQL_DATABASE)
    #logging.info(MYSQL_PASSWORD)
    logging.info(MYSQL_USER)

    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                host=hostSrv,
                                database=MYSQL_DATABASE)
    logging.info("Conexão efetuada com sucesso ao MySql!")                               

    #CUIDADO com o comando ACIMA - se o BD não aceitar multiplos cursores, é necessário abrir uma conexão dentro de cada função
except mysql.connector.Error as err:
    print("Erro na conexão com o BD - veja Log")
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        logging.info("Usuário ou senha inválido(s).")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        logging.error("Banco de dados não existe.")
    else:
        logging.error(err)
        logging.error("Erro na conexão com o Banco de Dados")
    sys.exit(1)

updater = Updater(token, use_context=True)  #para ser acessível ao disparador de mensagens

disparaAviso()
       
conn.close()       