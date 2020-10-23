"""
Created on Thu Jul 16 11:21:16 2020

@author: 53363833172
"""
from __future__ import unicode_literals
from datetime import datetime, timedelta
import re
import schedule
import time
from random import randint
import threading
import sys
import os
import logging   
import mysql.connector
from mysql.connector import errorcode
from telegram.ext import Updater
from telegram.ext import CommandHandler, CallbackQueryHandler, Filters, MessageHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton    
############################### Bot ############################################


#formata  o número do TDPF
def formataTDPF(tdpf): #passar uma string
    tdpf = tdpf.strip()
    if len(tdpf)<11: #tdpf deve ter 16 dígitos; testo se o tamanho é menor do que 11, pois referencio esta posição
        return tdpf
    return tdpf[:7]+"-"+tdpf[7:11]+"-"+tdpf[11:]
    
#verifica se um CPF é válido
def validaCPF(cpfPar):
#The MIT License (MIT) Copyright (c) 2015 Derek Willian Stavis
    cpf = getAlgarismos(cpfPar)
    if len(cpf)!=11:
        return False

    if cpf in [s * 11 for s in [str(n) for n in range(10)]]:
        return False

    calc = lambda t: int(t[1]) * (t[0] + 2)
    d1 = (sum(map(calc, enumerate(reversed(cpf[:-2])))) * 10) % 11
    d2 = (sum(map(calc, enumerate(reversed(cpf[:-1])))) * 10) % 11
    return str(d1) == cpf[-2] and str(d2) == cpf[-1]

def getAlgarismos(texto): #retorna apenas os algarismos de uma string
    algarismos = ""
    for car in texto:
        if car.isdigit():
            algarismos = algarismos + car
    return algarismos

def isDate(data): #verifica se a string é uma data válida
    if data==None:
        return False
    if len(data)!=10: #exige no formato dd/mm/aaaa
        return False
    # faz o split e transforma em números
    dia, mes, ano = map(int, data.split('/'))
    # mês ou ano inválido (só considera do ano 1 em diante), retorna False
    if mes < 1 or mes > 12 or ano <= 0:
        return False
    # verifica qual o último dia do mês
    if mes in (1, 3, 5, 7, 8, 10, 12):
        ultimo_dia = 31
    elif mes == 2:
        # verifica se é ano bissexto
        if (ano % 4 == 0) and (ano % 100 != 0 or ano % 400 == 0):
            ultimo_dia = 29
        else:
            ultimo_dia = 28
    else:
        ultimo_dia = 30
    # verifica se o dia é válido
    if dia < 1 or dia > ultimo_dia:
        return False
    return True

def getParametros(msg): #monta uma lista com os parâmetros a partir da msg do usuário
    msg = msg.strip()
    parametros = msg.split()
    i = 0
    tamLista = len(parametros)
    while i<tamLista:
        parametros[i] = parametros[i].strip()
        if parametros[i]=="":
            for j in range(i+1, tamLista):
                parametros[j-1] = parametros[j]
            parametros.pop()
            tamLista-=1
        i+=1    
    return parametros

def eliminaPendencia(userId):
    global pendencias #, cpfRegistro
    #apaga todas as pendencias (usuário retornou ao menu, acionou alguma opçaõ dele ou prestou todas as informações requeridas por uma funcionalidade)
    #logging.info(pendencias)
    if pendencias==None:
        return
    while pendencias.get(userId, "1")!="1": #há pendências (retona "1" em caso de não haver)
        try:
            del pendencias[userId]
        except:
            logging.info("Erro ao apagar pendencia do usuário "+str(userId))
            break
               
    #while cpfRegistro.get(userId, "1")!="1": #há pendências (retona "1" em caso de não haver)
    #    try:
    #        del cpfRegistro[userId]
    #    except:
    #        logging.info("Erro (3) ao apagar pendencia do usuário "+str(userId))
    #        break         
    return      

#transforma uma data string de dd/mm/yyyy para yyyy/mm/dd para fins de consulta, inclusão ou atualização no BD SQL
#se o BD esperar a data em outro formato, basta alterarmos aqui
def converteAMD(data):
    return data[6:]+"/"+data[3:5]+"/"+data[:2] 

  
def registra(update, context): #registra usuário no serviço
    global pendencias, textoRetorno, conn, d1padrao, d2padrao, d3padrao #, cpfRegistro
    response_message = ""
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=2:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o CPF e o código de registro (chave)."
        response_message = response_message+textoRetorno
    else:
        cpf = getAlgarismos(parametros[0])
        chave = getAlgarismos(parametros[1])                     
        logging.info(cpf + " "+chave)
        if not validaCPF(cpf):
            response_message = "CPF inválido. Envie novamente CPF e o código de registro (chave)."
            response_message = response_message+textoRetorno
        elif not chave.isdigit() or chave==None or chave=="":
            response_message = "Código de registro inválido. Envie novamente CPF e o código de registro (chave)."
            response_message = response_message+textoRetorno 
        else:
            try:
                chave = int(chave)       
            except:
                response_message = "Código de registro inválido. Envie novamente CPF e o código de registro (chave)."
                response_message = response_message+textoRetorno  
        if response_message!="":
            bot.send_message(userId, text=response_message)
            return 
        cursor = conn.cursor(buffered=True)
        cursor.execute("Select Codigo, CPF, Chave, Adesao from Usuarios where CPF=%s", (cpf,))  
        row = cursor.fetchone()
        if row:
            codigo = row[0]
            if row[2]==chave:
                eliminaPendencia(userId) #apaga a pendência de informação do usuário                
                try:
                    if row[3]==None: #usuário é novo no serviço (nunca havia se registrado)
                        comando =  "Update Usuarios set idTelegram=%s, Adesao=%s, Saida=Null, Tentativas=0, d1=%s, d2=%s, d3=%s where Codigo=%s"
                        cursor.execute(comando, (userId, datetime.now().date(), d1padrao, d2padrao, d3padrao, codigo))
                    else:   
                        comando =  "Update Usuarios set idTelegram=%s, Saida=Null, Tentativas=0 where Codigo=%s"
                        cursor.execute(comando, (userId, codigo))
                    conn.commit()
                    response_message = "Usuário registrado/reativado com sucesso."                     
                except:
                    conn.rollback()
                    response_message = "Erro ao registrar o usuário no serviço."  
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)    
                return
            else:
                response_message = "Gere a chave primeiramente ou digite-a corretamente. Digite novamente o CPF e o código de registro." +textoRetorno                                   
        else:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            response_message = "Usuário (CPF) não foi cadastrado para registro no serviço."
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)    
            return            
    bot.send_message(userId, text=response_message)
    return


def acompanha(update, context): #inicia o monitoramente de um ou de TODOS os TDPFs que o usuário supervisione ou em que esteja alocado
    global pendencias, textoRetorno, conn
    cursor = conn.cursor(buffered=True)    
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o nº do TDPF ou a palavra TODOS."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)
        return        
    else:  
        comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            cpf = row[0]
            if row[1]!=None: #saida
                eliminaPendencia(userId) #apaga a pendência de informação do usuário
                response_message = "Usuário não está ativo no serviço." #já testado quando acionou o menu
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)
                return                
        if not achou:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            response_message = "Usuário não está registrado no serviço." #tb já foi testado        
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            return             
        info = parametros[0]
        if info.upper().strip() in ["TODOS", "TODAS"]:
            comando = "Select TDPF from Alocacoes, TDPFs Where Desalocacao Is Null and CPF=%s and TDPF=Numero and Encerramento Is Null"
            cursor.execute(comando, (cpf,))
            tdpfs = cursor.fetchall()
            if not tdpfs:
                response_message = "Não há TDPFs ativos em que o CPF {} esteja alocado.".format(cpf) #tb já foi testado                        
        else:
            tdpf = getAlgarismos(info.strip())
            if len(tdpf)!=16 or not tdpf.isdigit():
                response_message = "TDPF ou comando inválido. Envie novamente o nº do TDPF (16 dígitos) ou a palavra TODOS."
                response_message = response_message+textoRetorno  
                bot.send_message(userId, text=response_message) 
                return                 
            comando = "Select TDPF from Alocacoes, TDPFs Where Desalocacao Is Null and CPF=%s and TDPF=%s and TDPF=Numero and Encerramento Is Null"
            cursor.execute(comando, (cpf, tdpf))
            tdpfs = cursor.fetchall()
            if not tdpfs:
                response_message = "TDPF não existe, usuário não é supervisor ou não está alocado nele."
        if not tdpfs:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            return
        atualizou = False 
        try:
            for tdpfObj in tdpfs:
                tdpf = tdpfObj[0]
                comando = "Select Codigo as codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
                cursor.execute(comando, (cpf, tdpf))
                row = cursor.fetchone()
                if row:
                    if row[1]!=None:
                        comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                        cursor.execute(comando, (row[0]))
                        atualizou = True
                else:
                    comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                    cursor.execute(comando, (cpf, tdpf, datetime.today().date()))
                    atualizou = True
            if atualizou:        
                conn.commit()
                response_message = "Operação efetivada com sucesso (houve atualização)."
            else:
                response_message = "Operação efetivada com sucesso (não houve atualização)."
        except:
            if atualizou:
                conn.rollback()
                response_message="Erro (1) ao tentar efetivar a operação. Tente novamente mais tarde."
            else:
                response_message="Erro (2) ao tentar efetivar a operação. Tente novamente mais tarde."                
        bot.send_message(userId, text=response_message) 
        eliminaPendencia(userId) #apaga a pendência de informação do usuário        
        mostraMenuPrincipal(update, context)
        return  
    
            
def efetivaCiencia(userId, tdpf, data): #tenta efetivar a ciência de um tdpf no BD
    global conn
    cursor = conn.cursor(buffered=True)  
    try:
        comando = "Select Encerramento from TDPFS Where Numero=%s"        
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                return False, "TDPF foi encerrado - monitoramento não é mais necessário ou possível."
        if not achou:    
            return False, "TDPF não foi localizado - não existe ou foi encerrado há um bom tempo e retirado da base deste serviço."
        comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            cpf = row[0]
            if row[1]!=None: #saida
                return False, "Usuário não está ativo no serviço." #já testado quando acionou o menu
        if not achou:
            return False, "Usuário não está registrado no serviço." #tb já foi testado
        comando = "Select Desalocacao from Alocacoes Where CPF=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()        
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                return False, "Usuário não está mais alocado ao TDPF."
        if not achou:
            return False, "Usuario não está alocado ao TDPF."
        comando = "Select Data from Ciencias Where TDPF=%s and Data>=%s Order by Data DESC"
        cursor.execute(comando, (tdpf, datetime.strptime(data, "%d/%m/%Y")))
        row = cursor.fetchone()
        if row:
            return False, "Data de ciência informada DEVE ser posterior à ultima informada para o TDPF ("+row[0].strftime('%d/%m/%Y')+")."
        comando = "Select Codigo, Fim from CadastroTDPFs Where TDPF=%s and Fiscal=%s"
        cursor.execute(comando, (tdpf, cpf))
        row = cursor.fetchone()        
        tdpfCadastrado = False
        fim = None
        if row:
            tdpfCadastrado = True  
            chave = row[0]
            fim = row[1]
            #logging.info("TDPF cadastrado")
            
    except:
       return False, "Erro na consulta (3)."
    try:
        comando = "Insert into Ciencias (TDPF, Data) Values (%s, %s)"
        cursor.execute(comando, (tdpf, datetime.strptime(data, "%d/%m/%Y")))
        msg = ""
        if fim!=None:
            msg = " Monitoramento deste TDPF foi reativado."
            comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
            cursor.execute(comando, (chave,))                         
        elif not tdpfCadastrado:
            msg = " Monitoramento deste TDPF foi iniciado."
            comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
            cursor.execute(comando, (cpf, tdpf, datetime.today().date()))
        conn.commit()
        return True, msg
    except:
        conn.rollback()
        return False, "Erro ao atualizar as tabelas."
            
    
def ciencia(update, context): #critica e tenta efetivar a ciência de um TDPF (registrar data)
    global pendencias, textoRetorno
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=2:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o nº do TDPF e a última data de ciência relativa a ele."
        response_message = response_message+textoRetorno
    else:
        tdpf = getAlgarismos(parametros[0])
        data = parametros[1]
        if data.isdigit() and len(data)==8:
            data = data[:2]+"/"+data[2:4]+"/"+data[4:]
        if len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF inválido. Envie novamente o TDPF (16 dígitos) e a data de ciência (separados por espaço)."
            response_message = response_message+textoRetorno            
        elif not isDate(data):
            response_message = "Data inválida. Envie novamente o TDPF e a data de ciência (dd/mm/aaaa) (separados por espaço)."
            response_message = response_message+textoRetorno
        else:
            try:
                dateTimeObj = datetime.strptime(data, '%d/%m/%Y')
            except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
                logging.info("Erro na conversão da data "+data+" - UserId "+str(userId))
                response_message = "Erro na conversão da data. Envie novamente o TDPF e a data de ciência (dd/mm/aaaa) (separados por espaço)."
                response_message = response_message+textoRetorno   
                bot.send_message(userId, text=response_message)  
                return                
            if dateTimeObj.date()>datetime.now().date():
                response_message = "Data de ciência não pode ser futura. Envie novamente o TDPF e outra data de ciência (separados por espaço)."
                response_message = response_message+textoRetorno                    
            elif dateTimeObj.date()<datetime.now().date()-timedelta(days=60):
                response_message = "Data de ciência já está vencida para fins de recuperação da espontaneidade tributária. Envie novamente o TDPF e outra data de ciência (separados por espaço)."
                response_message = response_message+textoRetorno
            else:    
                efetivou, msgCiencia = efetivaCiencia(userId, tdpf, data)
                if efetivou:
                    eliminaPendencia(userId) #apaga a pendência de informação do usuário
                    response_message = "Data de ciência registrada para o TDPF."
                    if msgCiencia!=None and msgCiencia!="":
                        response_message = response_message+msgCiencia
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)
                    return
                else:
                    response_message = msgCiencia+textoRetorno                              
    bot.send_message(userId, text=response_message)  
    return 

def efetivaPrazos(userId, d): #altera no BD os prazos padrão do usuário (d é uma lista)
    global conn
    cursor = conn.cursor(buffered=True) 
    #ordena os prazos (não tem utilidade, mas deixei assim pois pode ser que venha a ter)
    d.sort(reverse = True)
    d1 = d[0]
    d2 = d[1]
    d3 = d[2]            
    comando = "Update Usuarios set d1=%s, d2=%s, d3=%s where idTelegram=%s"
    try:
        logging.info(comando)
        cursor.execute(comando, (d1, d2, d3, userId))
        conn.commit()
        return True, "" #retorna se operação foi um sucesso e mensagem de erro em caso de False no primeiro         
    except:
        conn.rollback()
        return False, "Erro de atualização. Tente novamente mais tarde."
        
def prazos(update, context): #critica e tenta alterar os prazos padrão do usuário
    global pendencias, textoRetorno
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=3:
        response_message = "Número de informações (parâmetros) inválido. Envie 3 prazos em dias (1 a 50) antes da retomada da espontaneidade em que deseja ser alertado (separe cada um com um espaço)."
        response_message = response_message+textoRetorno
    else:
        d = parametros
        response_message = None
        if d[0].isdigit() and d[1].isdigit() and d[2].isdigit():
            diasInt = []
            for dn in d:
                dias = int(dn)
                diasInt.append(dias)
                if dias<1 or dias>50:
                    response_message = "Prazos devem estar compreendidos entre 1 e 50 dias. Envie 3 prazos em dias."
                    response_message = response_message+textoRetorno
                    break
            if not response_message:
                if d[0]==d[1] or d[1]==d[2] or d[0]==d[2]:
                    response_message = "Nenhum prazo pode ser igual a outro. Envie 3 prazos em dias diferentes um do outro."
                    response_message = response_message+textoRetorno                    
            if not response_message:
                efetivou, msgPrazos = efetivaPrazos(userId, diasInt)
                if efetivou:
                    eliminaPendencia(userId) #apaga a pendência de informação do usuário
                    response_message = "Prazos registrados."
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)
                    return
                else:
                    response_message = msgPrazos+textoRetorno
        else:
            response_message = "Os dias devem ser números inteiros entre 1 e 50. Envie 3 prazos em dias (1 a 50) antes da retomada da espontaneidade em que deseja ser alertado (separe cada um com um espaço)."
            response_message = response_message+textoRetorno
    bot.send_message(userId, text=response_message)  
    return

def efetivaAnulacao(userId, tdpf): #efetiva a anulação (apaga) de uma data de ciência de um tdpf - a data anterior, se houver, é que será considerada
    global conn
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row: #não achou usuário
            return False, "Usuário Telegram não está registrado no serviço."
        if row[1]!=None:
            return False, "Usuário Telegram saiu do serviço em "+row[1].strftime('%d/%m/%Y')+"."
        cpf = row[0]
        comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()
        if not row:
            return False, "TDPF não está sendo monitorado para você."
        if row[1]!=None:
            return False, "O acompanhamento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        comando = "Select Codigo, TDPF, Data from Ciencias Where TDPF=%s Order by Data"
        cursor.execute(comando, (tdpf,))
        rows = cursor.fetchall()
    except:
        return False, "Erro na consulta (5)."
    if len(rows)==0:
        return False, "Não há data de ciência informada para o TDPF." #Não havia data de ciência para o TDPF
    if len(rows)==1:
        dataAnt = "Nenhuma Data Vigente" #não haverá data anterior
    else: #2 ou mais linhas
        dataAnt = rows[len(rows)-2][2].strftime('%d/%m/%Y')        
    chave = rows[len(rows)-1][0]    
    try:
        comando = "Delete from Ciencias Where Codigo=%s"
        cursor.execute(comando, (chave,))
        conn.commit()   
        return True, dataAnt #retorna se teve sucesso e a data anterior
    except:
        conn.rollback()
        return False, "Erro na atualização das tabelas. Tente novamente mais tarde."

def anulaCiencia(update, context): #anula (apaga) a última data de ciência do TDPF
    global pendencias, textoRetorno
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Envie somente o nº do TDPF (16 dígitos), sem espaços."
        response_message = response_message+textoRetorno      
    else:
        tdpf = getAlgarismos(parametros[0])
        if len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF inválido. Envie novamente o TDPF (16 dígitos)."
            response_message = response_message+textoRetorno
        else:
            efetivou, msgAnulacao = efetivaAnulacao(userId, tdpf)
            eliminaPendencia(userId) #apaga a pendência de informação do usuário                
            if efetivou:
                if msgAnulacao!=None: #se efetivou e retornou mensagem, esta é a data da ciência anterior
                    response_message = "Última data de ciência foi excluída para o TDPF. Retornou para a anterior: "+msgAnulacao
                    try:
                        dateTimeObj = datetime.strptime(msgAnulacao, '%d/%m/%Y')
                        if dateTimeObj<datetime.now().date()-timedelta(days=59):
                            response_message = response_message+"\nA data de ciência anterior já está vencida e não irá gerar, na prática, monitoramento."
                    except :
                        response_message = response_message+"\nNa prática, não haverá agora monitoramento para este TDPF por falta de data de ciência."                   
                else:
                    response_message = "Este TDPF não estava sendo monitorado pelo serviço para você."
            else:
                response_message = msgAnulacao
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            return
    bot.send_message(userId, text=response_message)  
    return          

def efetivaFinalizacao(userId, tdpf=""): #finaliza monitoramento de um tdpf (campo Fim da tabela CadastroTDPFs)
    global conn
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row: #não achou usuário
            return False, "Usuário Telegram não está registrado no serviço."
        if row[1]!=None:
            return False, "Usuário Telegram saiu do serviço em "+row[1].strftime('%d/%m/%Y')+"."
        cpf = row[0]
        if tdpf!="":
            comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
            cursor.execute(comando, cpf, tdpf)            
        else:
            comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and Fim Is Null"
            cursor.execute(comando, (cpf,))
        row = cursor.fetchone()
        if not row:
            if tdpf!="":
                return False, "TDPF não está sendo monitorado por você. Envie um novo nº de TDPF:"
            else:
                return True, "Nenhum TDPF está sendo monitorado por você atualmente."
        if row[1]!=None and tdpf!="":
            return True, "O acompanhamento do TDPF já havia sido finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        elif tdpf!="":
            chave = row[0]
    except:
        return False, "Erro na consulta (7). Tente novamente mais tarde."
    try:
        if tdpf!="":
            comando = "Update CadastroTDPFs Set Fim=%s Where Codigo=%s"
            cursor.execute(comando, (datetime.today().date(), chave))
        else:
            comando = "Update CadastroTDPFs Set Fim=%s Where Fiscal=%s"
            cursor.execute(comando, (datetime.today().date(), cpf))
        conn.commit()            
        return True, None
    except:
        conn.rollback()
        return False, "Erro na atualização das tabelas (efetivaFinalizacao)."

def fim(update, context): #exclui o TDPF do monitoramento (campo Fim da tabela CadastroTDPFs)
    global pendencias, textoRetorno
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Envie somente o nº do TDPF (16 dígitos), sem espaços, ou a palavra TODOS."
        response_message = response_message+textoRetorno
    else:
        efetivou = None
        parametro = parametros[0].strip().upper()
        tdpf = getAlgarismos(parametro)
        if parametro in ["TODOS", "TODAS"]:
            efetivou, msgAnulacao = efetivaFinalizacao(userId)
        elif len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF ou comando inválido. Envie novamente o TDPF (16 dígitos), sem espaços, ou a palavra TODOS."
            response_message = response_message+textoRetorno
        else:
            efetivou, msgAnulacao = efetivaFinalizacao(userId, tdpf)
        if efetivou!=None:
            if efetivou:
                eliminaPendencia(userId) #apaga a pendência de informação do usuário
                if msgAnulacao==None or msgAnulacao=="":
                    response_message = "TDPF(s) excluído(s) do monitoramento deste serviço."
                else:
                    response_message = msgAnulacao
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)
                return
            else:
                response_message = msgAnulacao+textoRetorno            
    bot.send_message(userId, text=response_message)  
    return

def verificaEMail(email): #valida o e-mail se o usuário informou um completo
    regex1 = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    regex2 = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+[.]\w{2,3}$'  

    if(re.search(regex1,email)):  
        return True   
    elif(re.search(regex2,email)):  
        return True
    else:  
        return False 

def cadastraEMail(update, context): #cadastra o e-mail institucional
    global pendencias, textoRetorno, conn
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    response_message = ""
    if len(parametros)!=1:
        response_message = "Envie somente o nome de usuário do e-mail institucional @rfb.gov.br."
        response_message = response_message+textoRetorno
    else:        
        email = parametros[0].strip()
        if "@" in email:
            if not verificaEMail(email):
                response_message = "Email inválido. Envie somente o nome de usuário do e-mail institucional @rfb.gov.br."
                response_message = response_message+textoRetorno
            elif not "@rfb.gov.br" in email:
                response_message = "Email não é institucional. Envie somente o nome de usuário do e-mail institucional @rfb.gov.br."
                response_message = response_message+textoRetorno 
            else:
                email = email[:email.find("@")]
            logging.info(email) 
        elif len(email)<4:
            response_message = "Nome muito curto (min = 4 caracteres). Envie somente o nome de usuário do endereço @rfb.gov.br."
            response_message = response_message+textoRetorno 
        if response_message=="":    
            comando = "Select Codigo, CPF, email from Usuarios Where Saida Is Null and idTelegram=%s"
            cursor = conn.cursor(buffered=True)
            try:
                cursor.execute(comando, (userId,))
                row = cursor.fetchone()
            except:
                response_message = "Erro na consulta (6)."+textoRetorno
                bot.send_message(userId, text=response_message)  
                return                
            if not row:
                response_message = "Usuário não registrado ou inativo no serviço."
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)
                return                 
            else:
                chave = row[0]
                cpf = row[1]
                cpf = cpf[:3]+"."+cpf[3:6]+"."+cpf[6:9]+"-"+cpf[9:]
                emailAnt = row[2]
                if email.upper()=="NULO" and (emailAnt==None or emailAnt==""):
                    response_message = "Usuário não tinha e-mail cadastrado no serviço."
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)
                    return
                try:
                    if email.upper()=="NULO":
                        comando = "Update Usuarios Set email=Null Where Codigo=%s"   
                        cursor.execute(comando, (chave,))
                    else:    
                        comando = "Update Usuarios Set email=%s Where Codigo=%s"  
                        cursor.execute(comando, (email, chave))
                    conn.commit()
                    eliminaPendencia(userId) #apaga a pendência de informação do usuário                    
                    if emailAnt!=None and emailAnt!="":
                        if email.upper()=="NULO":
                            response_message = "Email anteriormente informado ("+emailAnt+"@rfb.gov.br) foi descadastrado."
                        else:    
                            response_message = "Email anteriormente cadastrado ("+emailAnt+"@rfb.gov.br) foi substituído.\n"                
                    if email.upper()!="NULO":
                        response_message = response_message+"Email cadastrado com sucesso - {}@rfb.gov.br.".format(email)    
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)
                    return 
                except:
                    response_message = "Erro na atualização das tabelas."+textoRetorno
    bot.send_message(userId, text=response_message)  
    return


def start(update, context):
    global pendencias
    
    userId = update.message.from_user.id  
    bot = update.effective_user.bot
    logging.info(update.message.from_user.first_name+" - "+str(userId))    
    if update.effective_user.is_bot:
        return #não atendemos bots     
    eliminaPendencia(userId)
    bot.send_message(userId, text='Este serviço monitora os prazos restantes para que o contribuinte readquira'+
                     ' a espontaneidade tributária dos TDPFs informados e avisa o usuário com antecedência'+
                     ' em 3 datas distintas. Digite a qualquer momento /menu para ver o menu principal.')
    mostraMenuPrincipal(update, context)
    return
  
def unknown(update, context):
    global pendencias, dispatch
    
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    logging.info(update.message.from_user.first_name+" - "+str(userId)) 
    if update.effective_user.is_bot:
        return #não atendemos bots 
    mensagem = update.message.text
    logging.info(mensagem)    
    if pendencias.get(userId, "1")!="1": #há pendências - então usuário tinha acionado uma opção do menu e preencheu informações (válidas = parâmetros, ou não) 
        dispatch[pendencias[userId]](update, context) #encaminha para a função adequada à pendência
    else:            
        response_message = update.message.from_user.first_name+", esta mensagem/comando não significou nada para mim. Mostrarei o menu para lhe auxiliar."  
        bot.send_message(userId, text=response_message)
        mostraMenuPrincipal(update, context) 
    return

def mostraMenuCadastro(update, context):
    global pendencias
    userId = update.effective_user.id   
    opcao1 =  tipoOpcao1(userId) 
    bot = update.effective_user.bot     
    if opcao1[:4] == 'Erro':
        bot.send_message(userId, text="Erro na consulta ao seu id")
        return
    menu = [[opcao1], ['Prazos Para Receber Avisos'], ['Cadastra/Exclui e-Mail'], ['Menu Principal']] 
    #mensagem = bot.send_message(userId, text="Teste apaga mensagem")
    #time.sleep(5)
    #bot.delete_message(userId, mensagem.message_id)
    #return
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu Cadastro:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True)) 
    return 

def mostraMenuTDPF(update, context):
    global pendencias
    menu = [['Informa Data de Ciência Relativa a TDPF', 'Anula Ciência Relativa a TDPF'], 
            ['Mostra TDPFs Monitorados', 'Mostra TDPFs Supervisionados'],
            ['Monitora TDPF(s)', 'Finaliza Monitoramento de TDPF'], 
            ['Menu Principal']]
    #userId = update.effective_user.id  
    #bot = update.effective_user.bot     
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu TDPF:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))  
    return  

def mostraMenuPrincipal(update, context):
    global pendencias
    #userId = update.effective_user.id     
    menu = [['Cadastros'], ['TDPF - Monitoramento']]    
    #bot = update.effective_user.bot     
    if update.effective_user.is_bot:
        return #não atendemos bots  
    update.message.reply_text("Menu Principal:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))
    return  

def tipoOpcao1(userId):
    global conn
    cursor = conn.cursor(buffered=True)
    #comando = "Select CPF, Saida from Usuarios Where idTelegram=?"
    comando = "Select CPF, Saida from Usuarios Where idTelegram="+str(userId)
    #cursor.execute(comando, float(userId))
    try:
        #cursor.execute(comando, float(userId))
        cursor.execute(comando)
        row = cursor.fetchone()
        if not row:
            return "Registra Usuário"
        else:
            if row[1]==None:
                return "Desativa Usuário"
            else:
                return "Reativa Usuário"
    except:
        return "Erro na consulta (tipoOpcao)"            


def opcaoUsuario(update, context): #Cadastra usuário
    global pendencias, conn
    
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    msgOpcao1 = tipoOpcao1(userId)
    if msgOpcao1=="Registra Usuário" or msgOpcao1=="Reativa Usuário":       
        pendencias[userId] = 'registra' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, seu CPF e o código de registro (chave) (separe as informações com espaço):"  
        bot.send_message(userId, text=response_message)
    else:
        cursor = conn.cursor(buffered=True)        
        if msgOpcao1=="Desativa Usuário":
            dataAtual = datetime.now().date()
            comando = "Update Usuarios Set Saida=%s Where idTelegram=%s"
            try:    
                cursor.execute(comando, (dataAtual, userId))
                conn.commit()
                response_message = "Usuário desativado."  
                bot.send_message(userId, text=response_message)                                 
            except:
                conn.rollback()
                response_message = "Erro ao atualizar tabelas(8). Tente novamente mais tarde."            
                bot.send_message(userId, text=response_message)
            mostraMenuPrincipal(update, context)                                          
    return

def verificaUsuario(userId, bot): #verifica se o usuário está cadastrado e ativo no serviço
    global conn, textoRetorno

    cursor = conn.cursor(buffered=True)
    comando = "Select CPF from Usuarios Where Saida Is Null and idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row:
            bot.send_message(userId, text="Usuário não está registrado no serviço ou está inativo. "+textoRetorno)  
            return False
        else:
            return True
    except:
        bot.send_message(userId, text="Erro na consulta (7).")        
        return False        
        

def opcaoInformaCiencia(update, context): #Informa ciência de TDPF
    global pendencias, conn    
    userId = update.effective_user.id  
    bot = update.effective_user.bot      
    eliminaPendencia(userId)     
    achou = verificaUsuario(userId, bot)       
    if achou:              
        pendencias[userId] = 'ciencia' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, o nº do TDPF (16 dígitos) e a data de ciência (dd/mm/aaaa) válida para fins de perda da espontaneidade tributária relativa ao respectivo procedimento fiscal - separe as informações com espaço:"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context) 
    return
        
def opcaoPrazos(update, context): #Informa prazos para receber avisos
    global pendencias, conn, textoRetorno  
    userId = update.effective_user.id 
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    comando = "Select d1, d2, d3, Saida from Usuarios Where idTelegram=%s"
    saida = None
    try:
        cursor = conn.cursor(buffered=True)    
        cursor.execute(comando, (userId,))
        achou = False
        row = cursor.fetchone()
        if row:
            achou = True
            d1 = row[0]
            d2 = row[1]
            d3 = row[2]
            saida = row[3] 
    except:
        response_message = "Erro na consulta. (5)"
        bot.send_message(userId, text=response_message)
        return             
    if not achou:
        response_message = "Usuário não está registrado no serviço. "+textoRetorno
    else:    
        if saida!=None:
            response_message = "Usuário está inativo no serviço. "+textoRetorno            
        else:    
            pendencias[userId] = 'prazos'   #usuário agora tem uma pendência de informação
            response_message = "Envie /menu para ver o menu principal. Prazos vigentes para receber alertas: {}, {} e {} dias antes de o contribuinte readquirir a espontaneidade.\nEnvie agora, numa única mensagem, três quantidades de dias (1 a 50) distintas antes de o contribuinte readquirir a espontaneidade tributária em que você deseja receber alertas (separe as informações com espaço):".format(d1, d2, d3)
    bot.send_message(userId, text=response_message)    
    return
        
def opcaoAnulaCiencia(update, context): #Anula ciência de TDPF
    global pendencias    
    userId = update.effective_user.id  
    bot = update.effective_user.bot    
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:            
        pendencias[userId] = 'anulaCiencia'  #usuário agora tem uma pendência de informação   
        response_message = "Envie /menu para ver o menu principal. Envie agora o nº do TDPF (16 dígitos) para o qual você deseja anular a última ciência informada que impedia a recuperação da espontaneidade (retornará para a anterior):"
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context)         
    return

def opcaoFinalizaAvisos(update, context): #Finaliza avisos para um certo TDPF
    global pendencias    
    userId = update.effective_user.id
    bot = update.effective_user.bot      
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'fim'     #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora o nº do TDPF (16 dígitos) ou a palavra TODOS para finalizar alertas/monitoramento de um ou de todos os TDPFs:"
        bot.send_message(userId, text=response_message)  
    else:
        mostraMenuPrincipal(update, context)         
    return

def opcaoAcompanhaTDPFs(update, context): #acompanha um TDPF ou todos os TDPFs em que estiver alocado ou em que for supervisor
    global pendencias    
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'acompanha' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora o nº do TDPF (16 dígitos) ou a palavra TODOS para receber alertas relativos ao TDPF informado ou a todos em que estiver alocado e/ou que for supervisor:"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context)         
    return
        
def montaListaTDPFs(userId, tipo=1):
    global conn
    try:
        cursor = conn.cursor(buffered=True)
        comando = "Select CPF from Usuarios Where idTelegram=%s and Saida Is Null"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row:
            return None
        cpf = row[0]
        if tipo==1:
        #seleciona monitoramentos ativos (incluído pelo usuário, ainda alocado nele e não encerrado)
            comando = '''Select CadastroTDPFs.TDPF as tdpf, TDPFs.Vencimento as Vencimento, Supervisor from CadastroTDPFs, Alocacoes, 
                        TDPFs Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.Fim Is Null and CadastroTDPFs.Fiscal=Alocacoes.CPF and 
                        CadastroTDPFs.TDPF=Alocacoes.TDPF and CadastroTDPFs.TDPF=TDPFs.Numero and Alocacoes.Desalocacao Is Null and 
                        TDPFs.Encerramento Is Null'''
        elif tipo==2:
        #seleciona todos os TDPFs dos quais o usuário é supervisor
            comando = '''Select Alocacoes.TDPF as tdpf, Vencimento from Alocacoes, TDPFs Where Desalocacao Is Null and Encerramento Is Null 
                        and CPF=%s and TDPF=Numero and Supervisor='S' Order by TDPF'''
        else:
            return None
        cursor.execute(comando, (cpf,))
        listaAux = cursor.fetchall()
        if not listaAux:
            return None
        result = []
    
        for linha in listaAux:
            tdpf = linha[0]
            vencimento = linha[1]
            if vencimento:
                vencimento = vencimento.date()
                vctoTDPF = str((vencimento-datetime.today().date()).days)
            else:
                vctoTDPF = "ND"
            comando = "Select Data from Ciencias Where TDPF=%s order by Data DESC"
            logging.info(comando)
            cursor.execute(comando, (tdpf,))            
            cienciaReg = cursor.fetchone() #busca a data de ciência mais recente (DESC acima)
            if tipo==2: #verificamos se o TDPF está sendo monitorado
                comando = "Select Codigo as codigo from CadastroTDPFs Where TDPF=%s and Fim Is Null"
                cursor.execute(comando, (tdpf,))
                monitoradoReg = cursor.fetchone()
                if monitoradoReg:
                    monitorado = "SIM"
                else:
                    monitorado = "NÃO"
            tdpf = formataTDPF(tdpf)
            if cienciaReg: 
                ciencia = cienciaReg[0] #obtem a data de ciência mais recente
            else:
                ciencia = None
            if tipo==1:
                registro = [tdpf, linha[2], ciencia, vctoTDPF]
            else:
                registro = [tdpf, monitorado, ciencia, vctoTDPF]
            result.append(registro)       
        if len(result)>0:
            logging.info(result)
            return result
        else:
            return None
    except:
        return ["Erro na consulta (6). Tente novamente mais tarde."]
            
        
def opcaoMostraTDPFs(update, context): #Relação de TDPFs e prazos
    global pendencias    
    userId = update.effective_user.id
    bot = update.effective_user.bot      
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        return        
    lista = montaListaTDPFs(userId, 1)
    if lista==None:
        response_message = "Você não monitora nenhum TDPF ou nenhum deles possui data de ciência informada neste serviço."        
    else:
        i = 1
        msg = ""
        for item in lista:
            tdpf = item[0]
            supervisor = item[1]
            if not supervisor:
                supervisor = "NÃO"
            if supervisor=="S":
                supervisor = "SIM"
            ciencia = item[2]
            vctoTDPF = item[3]
            if ciencia:
                delta = ciencia.date() + timedelta(days=60)-datetime.today().date()
                dias = delta.days
                if dias<0:
                    dias = "d) "+str(dias)+" (vencido); e) "+vctoTDPF
                else:
                    dias = "d) "+ str(dias) + "; e) "+vctoTDPF
                msg = msg+"\n\n"+str(i)+"a) "+tdpf+"; b) "+supervisor+";\nc) "+ciencia.strftime('%d/%m/%Y')+"; "+dias
            else:
                msg = msg+"\n\n"+str(i)+"a) "+tdpf+"; b) "+supervisor+"\nc) ND; d) ND; e) "+vctoTDPF
            i+=1                 
      
        response_message = "TDPFs Monitorados Por Você (somente):\na) TDPF; b) Supervisor; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Dias restantes para o vencto. do TDPF:"
        response_message = response_message+msg
    bot.send_message(userId, text=response_message)
    mostraMenuPrincipal(update, context)
    return

def opcaoMostraSupervisionados(update, context): #Relação de TDPFs supervisionados pelo usuário
    global pendencias    
    userId = update.effective_user.id
    bot = update.effective_user.bot      
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        mostraMenuPrincipal(update, context)         
        return        
    lista = montaListaTDPFs(userId, 2)
    if lista==None:
        response_message = "Você não supervisiona nenhum TDPF."        
    else:
        i = 1
        msg = ""
        for item in lista:
            logging.info(item)
            tdpf = item[0]
            monitorado = item[1]
            ciencia = item[2]
            vctoTDPF = item[3]
            if ciencia:
                delta = ciencia.date() + timedelta(days=60)-datetime.today().date()
                dias = delta.days
                if dias<0:
                    dias = " d) "+str(dias)+" (vencido); e) "+vctoTDPF
                else:
                    dias = " d) "+ str(dias) + "; e) "+vctoTDPF
                msg = msg+"\n\n"+str(i)+"a) "+tdpf+"; b) "+monitorado+";\nc) "+ciencia.strftime('%d/%m/%Y')+";"+dias
            else:
                msg = msg+"\n\n"+str(i)+"a) "+tdpf+"; b) "+monitorado+";\nc) ND; d) ND; e) "+vctoTDPF
            i+=1                      
        logging.info(lista)
        response_message = "TDPFs Supervisionados Por Você (somente):\na) TDPF; b) Monitorado Por Algum Fiscal; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Dias restantes para o vencto. do TDPF:"
        response_message = response_message+msg
    bot.send_message(userId, text=response_message)
    mostraMenuPrincipal(update, context)
    return
    
def opcaoEMail(update, context): #cadastra e-mail para o recebimento de avisos
    global pendencias, conn
    
    userId = update.effective_user.id
    bot = update.effective_user.bot       
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        mostraMenuPrincipal(update, context)         
        return     
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, email from Usuarios Where Saida Is Null and idTelegram=%s" 
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row:
            bot.send_message(userId, text="Usuário não está registrado no serviço ou está inativo. "+textoRetorno)
            return
        email = row.email
    except:
        bot.send_message(userId, text="Erro na consulta (7).")        
        return False     
    pendencias[userId] = 'email'     #usuário agora tem uma pendência de informação
    if email!=None and email!="":
        response_message = "Envie /menu para ver o menu principal. Email atualmente cadastrado - "+email+"@rfb.gov.br. Informe seu novo nome de usuário do endereço de e-mail institucional ou a palavra NULO para descadastrar o atual (exemplo - se seu e-mail é fulano@rfb.gov.br, envie fulano):"
    else:    
        response_message = "Envie /menu para ver o menu principal. Envie agora seu nome de usuário do endereço de e-mail institucional no qual você também receberá alertas (exemplo - se seu e-mail é fulano@rfb.gov.br, envie fulano):"
    bot.send_message(userId, text=response_message)  
    return  



############################# Handlers #########################################
def botTelegram():
    global updater, token
    updater = Updater(token, use_context=True) 
    
    updater.dispatcher.add_handler(CommandHandler('start', start))
    #updater.dispatcher.add_handler(CallbackQueryHandler(menuTDPF))    
    updater.dispatcher.add_handler(CommandHandler('menu', start))
    updater.dispatcher.add_handler(CommandHandler('retorna', start)) 
    
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Menu Principal'), mostraMenuPrincipal))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Cadastros'), mostraMenuCadastro))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('TDPF - Monitoramento'), mostraMenuTDPF))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Prazos Para Receber Avisos'), opcaoPrazos))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Registra Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Desativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Reativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Cadastra/Exclui e-Mail'), opcaoEMail))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Data de Ciência Relativa a TDPF'), opcaoInformaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Anula Ciência Relativa a TDPF'), opcaoAnulaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Finaliza Monitoramento de TDPF'), opcaoFinalizaAvisos))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Mostra TDPFs Monitorados'), opcaoMostraTDPFs))     
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Monitora TDPF\(s\)'), opcaoAcompanhaTDPFs))     
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Mostra TDPFs Supervisionados'), opcaoMostraSupervisionados)) 
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Inicio'), mostraMenuPrincipal))    


    updater.dispatcher.add_handler(MessageHandler(Filters.all, unknown))     
    
    updater.start_polling()
    logging.info('Serviço iniciado - '+datetime.now().strftime('%d/%m/%Y %H:%M'))
    #updater.idle()    #não é necessário pq o programa vai ficar rodando em loop infinito
################################################################################
   
def disparaMensagens():
    global updater, conn, termina
    logging.info("Acionado o disparo de mensagens - "+datetime.now().strftime('%d/%m/%Y %H:%M'))
    cursor = conn.cursor()
    comando = "Select idTelegram, CPF, d1, d2, d3 from Usuarios Where Saida Is Null"
    cursor.execute(comando)
    usuarios = cursor.fetchall()
    dataAtual = datetime.now()
    totalMsg = 0
    msgDisparadas = 0
    tdpfsAvisados = set()
    for usuario in usuarios: #percorremos os usuários ativos Telegram
        if termina: #programa foi informado de que é para encerrar (quit)
            return
        logging.info("Verificando disparo para "+usuario[1])
        #if usuario.CPF=="53363833172": #me excluo do envio de mensagens - só para testes
        #    continue
        #if usuario.CPF=="05517694675": #exclusão da Juliana
        #    continue
        #lista de ciências do usuário vencendo nos prazos por ele estabelecidos ou de TDPFs 
        #vencendo em prazo <= 8 dias (supondo que a extração ocorrerá a cada 7 dias)         
        listaUsuario = ""
        d1 = usuario[2]
        d2 = usuario[3]
        d3 = usuario[4]
        #selecionamos os TDPFs do usuário em andamento e monitorados (ativos) pelo serviço
        comando = """
                Select CadastroTDPFs.TDPF as tdpf, Ciencias.Data as data
                from CadastroTDPFs, TDPFS, Ciencias, Alocacoes
                Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.TDPF=Ciencias.TDPF
                and CadastroTDPFs.TDPF=TDPFS.Numero and CadastroTDPFs.TDPF=Alocacoes.TDPF and  
                CadastroTDPFs.Fiscal=Alocacoes.CPF and TDPFS.Encerramento Is Null and 
                CadastroTDPFs.Fim Is Null and Alocacoes.Desalocacao Is Null
                Order by CadastroTDPFs.TDPF, Ciencias.Data 
                """
        cursor.execute(comando, (usuario[1],))        
        fiscalizacao = cursor.fetchone()
        if fiscalizacao:
            tdpfAnt = fiscalizacao[0]
            dataCiencia = datetime.strptime("01/01/2000", "%d/%m/%Y")
            while fiscalizacao: #percorremos os TDPFs MONITORADOS do usuário
                if termina: #foi solicitado o término do bot
                    return            
                tdpf = fiscalizacao[0]        
                if tdpf!=tdpfAnt: #mudou TDPF, então é a última ciência
                    #dataObjCiencia = datetime.strptime(dataCiencia, '%d/%m/%Y')                
                    prazoRestante = (dataCiencia.date()+timedelta(days=60)-dataAtual.date()).days                
                    if prazoRestante==d1 or prazoRestante==d2 or prazoRestante==d3:
                        if len(listaUsuario)==0:
                            listaUsuario = "Alertas do dia (TDPF | Dias*):"
                        listaUsuario = listaUsuario+"\n"+formataTDPF(tdpfAnt)+ " | "+str(prazoRestante)+" (a)"
                    tdpfAnt = tdpf 
                dataCiencia = fiscalizacao[1]                         
                fiscalizacao = cursor.fetchone()            
            #quando sai do loop, tem que fazer para o último, pois, em relação a ele, não vai haver mudança de TDPF    
            prazoRestante = (dataCiencia.date()+timedelta(days=60)-dataAtual.date()).days        
            if prazoRestante==d1 or prazoRestante==d2 or prazoRestante==d3:
                if len(listaUsuario)==0:
                    listaUsuario = "Alertas do dia (TDPF | Dias*):"
                listaUsuario = listaUsuario+"\n"+formataTDPF(tdpf)+ " | "+str(prazoRestante)+" (a)"             

        #selecionamos as datas de vencimento dos TDPFs em que o usuário está alocado, mesmo que não monitorados
        comando = """
                Select TDPFs.Numero as tdpf, TDPFs.Vencimento as vencimento, TDPFs.AvisouVencimento as avisou from TDPFs, Alocacoes
                Where Alocacoes.CPF=%s and TDPFs.Numero=Alocacoes.TDPF and TDPFs.Encerramento Is Null and 
                Alocacoes.Desalocacao Is Null
                """
        cursor.execute(comando, (usuario[1],))                
        tdpfUsuario = cursor.fetchone()
        while tdpfUsuario: #percorremos os TDPFs do usuário (TODOS em andamento no qual o usuário esteja atualmente alocado)
            vencimento = tdpfUsuario[1]
            avisou = tdpfUsuario[2]
            tdpf = tdpfUsuario[0]
            if vencimento: #não deve ser nulo, mas garantimos ...
                vencimento = vencimento.date()
                prazoVenctoTDPF = (vencimento-dataAtual.date()).days
                #não avisamos do vencimento de TDPF recentemente avisado (prazo: 7 dias - depende da carga)
                if not avisou:
                    podeAvisar = True
                else:
                    avisou = avisou.date()
                    if (avisou+timedelta(days=7))<dataAtual.date(): #vai depender da periodicidade da extração e carga
                        podeAvisar = True
                    else:
                        podeAvisar = False
                if (1<prazoVenctoTDPF<=15) and podeAvisar: #verificar este prazos quando for colocar em produção
                    if len(listaUsuario)==0:
                        listaUsuario = "Alertas do dia (TDPF | Dias*):"
                    tdpfsAvisados.add(tdpf)
                    listaUsuario = listaUsuario+"\n"+formataTDPF(tdpf)+ " | "+str(prazoVenctoTDPF)+" (b)"
            tdpfUsuario = cursor.fetchone()                    

        if len(listaUsuario)>0:
            listaUsuario = listaUsuario+"\n*Dias restantes"
            listaUsuario = listaUsuario+"\n(a) P/ recuperação da espontaneidade tributária."
            listaUsuario = listaUsuario+"\n(b) P/ vencimento do TDPF no Ação Fiscal."
            logging.info("Disparando mensagem para "+usuario[1])
            updater.bot.send_message(usuario[0], text=listaUsuario)   
            totalMsg+=1
            msgDisparadas+=1
            if msgDisparadas>=30:
                msgDisparadas = 0
                time.sleep(1) #a cada 30 mensagens, dormimos um segundo (limitação do Bot é 30 por seg - TESTE)
            logging.info(listaUsuario) 

    logging.info("Total de mensagens disparadas: "+str(totalMsg))        
    #se avisamos do vencimento do TDPF, colocamos a data na tabela para não avisarmos novamente
    #todo dia (espera uma semana)            
    lista = []
    for tdpf in tdpfsAvisados:
        tupla = (datetime.today().date(), tdpf)
        lista.append(tupla)
    if len(lista)>0:    
        comando = ("Update TDPFs Set AvisouVencimento=%s Where Numero=%s") 
        try:
            cursor.executemany(comando, lista)
            conn.commit()
        except:
            logging.info("Erro ao tentar atualizar as datas de aviso na tabela TDPFs.")
            conn.rollback()
    return


def disparador():
    global termina
    while not termina:
        schedule.run_pending() 
        time.sleep(60)
        #time.sleep(24*60*60) #dorme por 24 horas até verificar se deve fazer o disparo de mensagens        
    return    

sistema = sys.platform.upper()
if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
    hostSrv = 'localhost'
    dirLog = 'log\\'
else:
    hostSrv = 'mysqlsrv'
    dirLog = '/Log/'  

#print(datetime.now().strftime('%Y-%m-%d %H_%M')+' BotLog'+sistema+'.log')
    
logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Bot'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO)

MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "testedb")
MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234")
token = os.getenv("TOKEN", "ERRO")

if token=="ERRO":
    logging.error("Token do Bot Telegram não foi fornecido.")
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
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        logging.info("Usuário ou senha inválido(s).")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        logging.error("Banco de dados não existe.")
    else:
        logging.error(err)
        logging.error("Erro na conexão com o Banco de Dados")
        sys.exit(1)

#dias de antecedência padrão para avisar
d1padrao = 30
d2padrao = 20
d3padrao = 5

#tdpfs = {}

pendencias = {} #indica que próxima função deve ser chamada para analisar entrada de dados
#cpfRegistro = {} #guarda o cpf do usuário que está se registrando (será usado após validação da chave)
#encaminha a pendência para a função adequada para tratar o input do usuário
dispatch = { 'registra': registra, 'ciencia': ciencia, 'prazos': prazos, 'acompanha': acompanha,
             'anulaCiencia': anulaCiencia, 'fim': fim, 'email': cadastraEMail}
textoRetorno = "\nEnvie /menu para retornar ao menu principal"
updater = None #para ser acessível ao disparador de mensagens
#schedule.every().day.at("08:30").do(disparaMensagens)
schedule.every(1).minutes.do(disparaMensagens)
termina = False
botTelegram()
threadDisparador = threading.Thread(target=disparador, daemon=True) #encerra thread quando sair do programa sem esperá-la
threadDisparador.start()
while not termina:
    entrada = input("Digite QUIT para terminar o serviço BOT: ")
    if entrada:
        if entrada.strip().upper()=="QUIT":
            termina = True
updater.stop() 
schedule.clear()        
conn.close()       