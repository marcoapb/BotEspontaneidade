# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 09:26:09 2020

@author: 53363833172
"""

from __future__ import unicode_literals
import pandas as pd
import numpy as np
import sys
import os
import logging  
import mysql.connector
from mysql.connector import errorcode
import schedule
import threading
import time
from datetime import datetime, timedelta


def getAlgarismos(texto): #retorna apenas os algarismos de uma string
    algarismos = ""
    for car in texto:
        if car.isdigit():
            algarismos = algarismos + car
    return algarismos

def paraData(data):
    if not data:
        return None
    tipo = str(type(data)).upper()
    if "DATE" in tipo:
        return data
    if "STR" in tipo or "UNICODE" in tipo:
        return datetime.strptime(data, "%d/%m/%Y")
    if "TIMESTAMP" in tipo:
        return pd.to_datetime(data)
    return None

def realizaCargaDados():
    global dirExcel, termina

    try:
        dfTdpf = pd.read_excel(dirExcel+"TDPFS.xlsx")
        dfAloc = pd.read_excel(dirExcel+"Alocacoes.xlsx")
        dfFiscais = pd.read_excel(dirExcel+"Fiscais.xlsx")
        dfFiscais['CPF']=dfFiscais['CPF'].astype(str) 
    except:
        logging.info("Arquivos Excel não foram encontrados - TDPFs.xlsx, Alocacoes.xlsx ou Fiscais.xlsx; outra tentativa será feita em 12h") 
        return    

    MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "testedb")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234") 

    try:
        logging.info("Conectando ao servidor de banco de dados ...")
        logging.info(MYSQL_DATABASE)
        #logging.info(MYSQL_PASSWORD)
        logging.info(MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv,
                                    database=MYSQL_DATABASE)
        logging.info("Conexão efetuada com sucesso ao MySql!")                               
    except mysql.connector.Error as err:
        print("Erro na conexão com o BD - veja Log: "+datetime.now().strftime('%d/%m/%Y %H:%M'))
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Usuário ou senha inválido(s).")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Banco de dados não existe.")
        else:
            logging.error(err)
            logging.error("Erro na conexão com o Banco de Dados")
        return

    cursor = conn.cursor(buffered=True)


    #logging.info(dfTdpf.head())

    selectFisc = "Select * from Fiscais Where CPF=%s"
    insereFisc = "Insert Into Fiscais (CPF, Nome) Values (%s, %s)"

    selectTDPF = "Select Codigo, Grupo, Encerramento from TDPFS Where Numero=%s"
    insereTDPF = "Insert Into TDPFS (Numero, Grupo, Emissao, Nome, NI, Vencimento) Values (%s, %s, %s, %s, %s, %s)"
    atualizaTDPFEnc = "Update TDPFS Set Encerramento=%s Where Codigo=%s"
    atualizaTDPFGrupoVencto = "Update TDPFS Set Grupo=%s, Vencimento=%s Where Codigo=%s"

    selectAloc = "Select Codigo, Desalocacao from Alocacoes Where TDPF=%s and CPF=%s"
    insereAloc = "Insert Into Alocacoes (TDPF, CPF, Alocacao) Values (%s, %s, %s)"
    insereAlocDesaloc = "Insert Into Alocacoes (TDPF, CPF, Alocacao, Desalocacao) Values (%s, %s, %s, %s)"
    atualizaAloc = "Update Alocacoes Set Desalocacao=%s Where Codigo=%s"

    selectCiencias = "Select * from Ciencias Where TDPF=%s" 
    insereCiencia = "Insert Into Ciencias (TDPF, Data) Values (%s, %s)"

    selectUsuario = "Select Codigo, CPF, email from Usuarios Where CPF=%s"
    insereUsuario = "Insert Into Usuarios (CPF, email) Values (%s, %s)"
    updateUsuario = "Update Usuarios Set email=%s Where Codigo=%s"
                    
    logging.info(f"TDPFs: {dfTdpf.shape[0]} linhas e {dfTdpf.shape[1]} colunas")
    logging.info(f"AFRFBs Execução: {dfAloc.shape[0]} linhas e {dfAloc.shape[1]} colunas")
    tabFiscais=0
    tabTdpfs=0
    tabTdpfsAtu=0
    tabCiencias=0
    tabAloc=0
    tabAlocAtu=0
    tabUsuarios=0
    tabUsuariosAtu=0
    gruposAtu=0
    if termina:
        return
    logging.info("Iniciando loop na carga.")
    for linha in range(dfTdpf.shape[0]):
        atualizou = False
        tdpfAux = dfTdpf.iloc[linha,0]
        tdpf = getAlgarismos(tdpfAux)
        distribuicao = dfTdpf.iloc[linha, 9]
        inicio = dfTdpf.iloc[linha, 10]
        encerramento = dfTdpf.iloc[linha, 11]
        situacao = dfTdpf.iloc[linha, 13]
        ni = dfTdpf.iloc[linha, 16]
        nome = dfTdpf.iloc[linha, 17]
        cursor.execute(selectTDPF, (tdpf,))
        regTdpf = cursor.fetchone()    
        if not regTdpf and encerramento!="SD" and encerramento!="":    #TDPF encerrado e não existente na base - pulamos
            continue
        if regTdpf:
            if regTdpf[2]!=None:
                continue #TDPF já encerrado na base - não há interesse em atualizar
        df = dfAloc.loc[dfAloc['Número do RPF Expresso']==tdpfAux]
        for linha2 in range(df.shape[0]):
            grupo = df.iat[linha2, 4]
            grupo = getAlgarismos(grupo)
            cpf = getAlgarismos(df.iat[linha2, 6])
            fiscal = df.iat[linha2, 7]
            alocacao = df.iat[linha2, 8]
            desalocacao = df.iat[linha2, 9]
            dfFiscal = dfFiscais.loc[dfFiscais['CPF']==cpf]
            email = None
            if dfFiscal.shape[0]>0:
                if dfFiscal.iat[0, 4]!=np.nan and dfFiscal.iat[0, 4]!="": #email está na coluna 4 (coluna 'E' do Excel)
                    email = dfFiscal.iat[0, 4]
            print(email)        
            cursor.execute(selectUsuario, (cpf,))
            regUser = cursor.fetchone()
            if len(regUser)>0: #achou o usuário - vemos se tem e-mail cadastrado
                if (regUser[2]==None or regUser[2]=='') and email!=None: #regUser[2] é o email
                    cursor.execute(updateUsuario, (email, regUser[0]))
                    tabUsuariosAtu+=1
                    atualizou = True
            else: #inserimos o novo usuário
                cursor.execute(insereUsuario, (cpf, email))       
                tabUsuarios+=1 
                atualizou = True
            cursor.execute(selectFisc, (cpf,))
            regFisc = cursor.fetchone()
            if not regFisc:
                tabFiscais+=1
                atualizou = True
                cursor.execute(insereFisc, (cpf, fiscal))
            cursor.execute(selectAloc, (tdpf, cpf))
            regAloc = cursor.fetchone()
            if not regAloc:
                tabAloc+=1
                if desalocacao=="SD" or desalocacao=="":
                    atualizou = True
                    cursor.execute(insereAloc, (tdpf, cpf, paraData(alocacao)))
                else:
                    atualizou = True
                    cursor.execute(insereAlocDesaloc, (tdpf, cpf, paraData(alocacao), paraData(desalocacao)))
            elif regAloc[1]==None and desalocacao!="SD" and desalocacao!="":
                tabAlocAtu+=1
                atualizou = True
                cursor.execute(atualizaAloc, (paraData(desalocacao), regAloc[0]))
        if distribuicao:
            distData = paraData(distribuicao) 
            vencimento = distData + timedelta(days=120)
            while vencimento.date()<datetime.now().date():
                vencimento = vencimento + timedelta(days=120)
        if not regTdpf:
            tabTdpfs+=1
            atualizou = True
            cursor.execute(insereTDPF, (tdpf, grupo, distData, nome, ni, vencimento))
            if inicio!="SD" and inicio!="":
                tabCiencias+=1
                cursor.execute(insereCiencia, (tdpf, paraData(inicio)))
        elif regTdpf[2]==None and encerramento!="SD" and encerramento!="":
            tabTdpfsAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFEnc, (paraData(encerramento), regTdpf[0]))
        elif regTdpf[1]!=grupo:
            gruposAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFGrupoVencto, (grupo, vencimento, regTdpf[0]))
        if regTdpf and inicio!="SD" and inicio!="":
            cursor.execute(selectCiencias, (tdpf,))
            regCiencia = cursor.fetchone()
            if not regCiencia:
                tabCiencias+=1
                atualizou = True
                cursor.execute(insereCiencia, (tdpf, paraData(inicio)))
        if atualizou:        
            conn.commit()    
    cursor.close()
    conn.close()  

    logging.info("Registros Incluídos:")  
    logging.info(f"TDPFs: {tabTdpfs}")
    logging.info(f"Fiscais: {tabFiscais}")
    logging.info(f"Ciencias: {tabCiencias}")
    logging.info(f"Alocacoes: {tabAloc}")
    logging.info(f"Usuarios: {tabUsuarios}")    

    logging.info("Registros Atualizados:")
    logging.info(f"TDPFs: {tabTdpfsAtu}")
    logging.info(f"Grupos(TDPFs): {gruposAtu}")
    logging.info(f"Alocacoes: {tabAlocAtu}")
    logging.info(f"Usuarios: {tabUsuariosAtu}")

    try:
        os.rename(dirExcel+"TDPFS.xlsx", dirExcel+"TDPFS_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        os.rename(dirExcel+"Alocacoes.xlsx", dirExcel+"Alocacoes_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        logging.info("Arquivos renomeados")
    except:
        logging.error("Erro ao tentar renomear os arquivos")

    return


def disparador():
    global termina
    while not termina:
        schedule.run_pending() 
        time.sleep(12*60*60) #espera 12 horas para tentar realizar outra carga        
    return 

    
sistema = sys.platform.upper()

if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
    hostSrv = 'localhost'
    dirLog = 'log\\'
    dirExcel = 'Excel\\'
else:
    hostSrv = 'mysqlsrv'
    dirLog = '/Log/' 
    dirExcel = '/Excel/'

logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Carga'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO)
schedule.every(12).hours.do(realizaCargaDados) #a cada 12 horas, verifica se há arquivos para fazer a carga
termina = False
threadDisparador = threading.Thread(target=disparador, daemon=True) #encerra thread quando sair do programa sem esperá-la
threadDisparador.start()
realizaCargaDados() #faz a primeira tentativa de carga das planilhas logo no acionamento do programa
while not termina:
    entrada = input("Digite QUIT para terminar o serviço BOT: ")
    if entrada:
        if entrada.strip().upper()=="QUIT":
            termina = True
schedule.clear()        