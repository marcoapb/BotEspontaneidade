# -*- coding: utf-8 -*-
"""
Created on Mon Aug 24 09:26:09 2020

@author: 53363833172
"""

from __future__ import unicode_literals
from re import S
import pandas as pd
import numpy as np
import sys
import os
import mysql.connector
from mysql.connector import errorcode
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
        try:
            return datetime.strptime(data, "%d/%m/%Y")
        except:
            return None    
    if "TIMESTAMP" in tipo:
        try:
            return pd.Timestamp.to_pydatetime(data)
        except:
            return None  
    return None

def realizaAjuste():
    global dirExcel, termina, hostSrv, hora1

    dfOperacoes = pd.read_excel(dirExcel+"OPERACOES.xlsx")
    print(dfOperacoes.dtypes)
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234")     
    conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD, host=hostSrv, database=MYSQL_DATABASE)    
    cursor = conn.cursor(buffered=True)
    consultaTdpf = "Select TDPFS.Codigo From TDPFS Where TDPFS.Numero=%s"
    consultaOp = """
                   Select Operacoes.Codigo 
                   from Operacoes, OperacoesFiscais, Tributos 
                   Where Operacoes.TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo and 
                   Operacoes.Tributo=Tributos.Codigo and OperacoesFiscais.Operacao=%s and Tributos.Tributo=%s
                 """
    consultaTrib = "Select Codigo from Tributos Where Tributo=%s"
    consultaOpFisc = "Select Codigo from OperacoesFiscais Where Operacao=%s"
    incluiOp = "Insert Into Operacoes (TDPF, Operacao, PeriodoInicial, PeriodoFinal, Tributo) Values (%s, %s, %s, %s, %s)"
    linhasIncluidas=0
    for linha in range(dfOperacoes.shape[0]):
        tdpf = getAlgarismos(dfOperacoes.iat[linha, 0])
        cursor.execute(consultaTdpf, (tdpf,))
        rowTdpf = cursor.fetchone()
        if not rowTdpf:
            continue
        codTdpf = rowTdpf[0]
        tributo = int(dfOperacoes.iat[linha, 1])
        periodoInicial = paraData(dfOperacoes.iat[linha, 4])
        periodoFinal = paraData(dfOperacoes.iat[linha, 5])
        operacao = int(dfOperacoes.iat[linha, 8])
        cursor.execute(consultaOp, (codTdpf, operacao, tributo))
        rowOp = cursor.fetchone()
        if not rowOp:
            cursor.execute(consultaTrib, (tributo,))
            rowTrib = cursor.fetchone()
            cursor.execute(consultaOpFisc, (operacao,))
            rowOpFisc = cursor.fetchone()
            cursor.execute(incluiOp, (codTdpf, rowOpFisc[0], periodoInicial, periodoFinal, rowTrib[0]))
            linhasIncluidas+=1
    conn.commit()
    print("Linhas incluídas: ", linhasIncluidas)
    return


sistema = sys.platform.upper()

if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
    hostSrv = 'localhost'
    dirExcel = 'Excel\\'
else:
    hostSrv = 'mysqlsrv'
    dirExcel = '/Excel/'
realizaAjuste()