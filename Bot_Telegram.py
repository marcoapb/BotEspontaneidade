"""
Created on Thu Jul 16 11:21:16 2020

@author: 53363833172
"""

from __future__ import unicode_literals
from datetime import datetime, timedelta
import re
import schedule
import time
#from random import randint
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
    try:    
        # faz o split e transforma em números
        dia, mes, ano = map(int, data.split('/'))
    except:
        return False 
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
    global pendencias, atividadeTxt
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
               
    while atividadeTxt.get(userId, "1")!="1": #há pendência (retona "1" em caso de não haver) de informação do texto da atividade
        try:
            del atividadeTxt[userId]
        except:
            logging.info("Erro (3) ao apagar pendencia do usuário "+str(userId))
            break         
    return      

#transforma uma data string de dd/mm/yyyy para yyyy/mm/dd para fins de consulta, inclusão ou atualização no BD SQL
#se o BD esperar a data em outro formato, basta alterarmos aqui
def converteAMD(data):
    return data[6:]+"/"+data[3:5]+"/"+data[:2] 

  
def registra(update, context): #registra usuário no serviço
    global pendencias, textoRetorno, d1padrao, d2padrao, d3padrao #, cpfRegistro   
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
        conn = conecta()
        if not conn:   
            bot.send_message(userId, text="Erro na conexão - registra")  
            eliminaPendencia(userId)
            mostraMenuPrincipal(update, context)                    
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
                conn.close()  
                return
            else:
                conn.close()
                response_message = "Gere a chave primeiramente ou digite-a corretamente. Digite novamente o CPF e o código de registro." +textoRetorno                                   
        else:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            response_message = "Usuário (CPF) não foi cadastrado para registro no serviço."
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)   
            conn.close() 
            return            
    bot.send_message(userId, text=response_message)
    return


def acompanha(update, context): #inicia o monitoramente de um ou de TODOS os TDPFs que o usuário supervisione ou em que esteja alocado
    global pendencias, textoRetorno
    conn = conecta()
    if not conn: 
        response_message = "Erro na conexão - acompanha."
        bot.send_message(userId, text=response_message)
        eliminaPendencia(userId)
        mostraMenuPrincipal(update, context)
        return    
    cursor = conn.cursor(buffered=True)    
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o nº do TDPF ou a palavra TODOS."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)
        conn.close()
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
                conn.close()
                return                
        if not achou:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            response_message = "Usuário não está registrado no serviço." #tb já foi testado        
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            conn.close()
            return             
        info = parametros[0]
        if info.upper().strip() in ["TODOS", "TODAS"]:
            comando = "Select TDPF from Alocacoes, TDPFS Where Desalocacao Is Null and CPF=%s and TDPF=Numero and Encerramento Is Null"
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
                conn.close()
                return                 
            comando = "Select TDPF from Alocacoes, TDPFS Where Desalocacao Is Null and CPF=%s and TDPF=%s and TDPF=Numero and Encerramento Is Null"
            cursor.execute(comando, (cpf, tdpf))
            tdpfs = cursor.fetchall()
            if not tdpfs:
                response_message = "TDPF não existe, usuário não é supervisor ou não está alocado nele."
        if not tdpfs:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            conn.close()
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
        conn.close()
        return  

def efetivaAtividade(userId, tdpf, atividade, data): #tenta efetivar uma atividade para um certo tdpf no BD
    conn = conecta()
    if not conn:         
        return False, "Erro na conexão - efetivaAtividade"
    cursor = conn.cursor(buffered=True)  
    try:
        comando = "Select Encerramento from TDPFS Where Numero=%s"        
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                conn.close()
                return False, "TDPF foi encerrado - monitoramento não é mais necessário ou possível, inclusive registro de atividade."
        if not achou:    
            conn.close()
            return False, "TDPF não foi localizado - não existe ou foi encerrado há um bom tempo e retirado da base deste serviço."
        comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            cpf = row[0]
            if row[1]!=None: #saida
                conn.close()
                return False, "Usuário não está ativo no serviço." #já testado quando acionou o menu
        if not achou:
            conn.close()
            return False, "Usuário não está registrado no serviço." #tb já foi testado
        comando = "Select Desalocacao from Alocacoes Where CPF=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()        
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                conn.close()
                return False, "Usuário não está mais alocado ao TDPF."
        if not achou:
            conn.close()
            return False, "Usuario não está alocado ao TDPF."
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
        conn.close()
        return False, "Erro na consulta (efetivaAtividade)."
    try:
        if isDate(data):
            dataObj = datetime.strptime(data, "%d/%m/%Y")
        else:
            dataObj = datetime.today().date()+timedelta(days=int(data))    
        comando = "Insert into Atividades (TDPF, Atividade, Data) Values (%s, %s, %s)"
        cursor.execute(comando, (tdpf, atividade.upper(), dataObj))
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
        conn.close()
        return True, msg
    except:
        conn.rollback()
        conn.close()
        return False, "Erro ao atualizar as tabelas (efetivaAtividade)."

def atividadeTexto(update, context): #obtém a descrição da atividade e chama a função que grava no BD
    global pendencias, textoRetorno, atividadeTxt
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    if len(msg)<3:
        response_message = "Descrição inválida (menos de 3 caracteres). Envie somente a descrição do texto ou 'cancela'(sem aspas) para cancelar."
        response_message = response_message+textoRetorno
    else:
        atividade = msg.upper()
        if atividade=="CANCELA":
            eliminaPendencia(userId)
            mostraMenuPrincipal(update, context)
            return     
        efetivou, msgAtividade = efetivaAtividade(userId, atividadeTxt[userId][0], atividade, atividadeTxt[userId][1])
        if efetivou:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário
            response_message = "Atividade registrada para o TDPF."
            if msgAtividade!=None and msgAtividade!="":
                response_message = response_message+msgAtividade
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            return
        else:
            response_message = msgAtividade+textoRetorno  
    bot.send_message(userId, text=response_message)  
    return                

def atividade(update, context): #critica e tenta efetivar a realização de uma atividade com prazo a vencer
    global pendencias, textoRetorno, atividadeTxt
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=2:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o nº do TDPF e a data ou o prazo de vencimento em dias."
        response_message = response_message+textoRetorno
    else:
        response_message = ""
        tdpf = getAlgarismos(parametros[0])
        data = parametros[1]
        if data.isdigit() and len(data)==8:
            data = data[:2]+"/"+data[2:4]+"/"+data[4:]            
        if len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF inválido. Envie novamente o TDPF (16 dígitos) e a data/prazo de vencimento (separados por espaço)."
            response_message = response_message+textoRetorno                          
        elif not isDate(data) and not data.isdigit():
            response_message = "Data/prazo inválido. Envie novamente o TDPF e a data/prazo de vencimento (separados por espaço)."
            response_message = response_message+textoRetorno                       
        if len(response_message)==0:
            prazo = 0
            dateTimeObj = None
            if isDate(data):
                try:
                    dateTimeObj = datetime.strptime(data, '%d/%m/%Y')
                except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
                    logging.info("Erro na conversão da data "+data+" - UserId "+str(userId))
                    response_message = "Erro na conversão da data. Envie novamente o TDPF e a data de vencimento (dd/mm/aaaa) ou prazo de vencimento em dias (separados por espaço)."
                    response_message = response_message+textoRetorno   
            else:
                try:
                    prazo = int(data)  
                    if prazo>365 or prazo<=0:
                        response_message = "Prazo de vencimento deve ser superior a 0 e inferior a 366 dias. Envie novamente o TDPF e a data de vencimento ou prazo de vencimento em dias (separados por espaço)."
                        response_message = response_message+textoRetorno                          
                except: 
                    logging.info("Erro na conversão do prazo "+data+" - UserId "+str(userId))
                    response_message = "Erro na conversão do prazo. Envie novamente o TDPF e a data/prazo de vencimento (separados por espaço)."
                    response_message = response_message+textoRetorno   
            if dateTimeObj!=None and len(response_message)==0:                                       
                if dateTimeObj.date()<=datetime.now().date():
                    response_message = "Data de vencimento deve ser futura. Envie novamente o TDPF e outra data/prazo de vencimento (separados por espaço)."
                    response_message = response_message+textoRetorno                    
            if len(response_message)==0:
                eliminaPendencia(userId)
                pendencias[userId] = 'atividadeTexto'
                atividadeTxt[userId] = [tdpf, data]
                response_message = "Informe a descrição da atividade (máximo de 50 caracteres)."                     
    bot.send_message(userId, text=response_message)  
    return 

def efetivaAnulacaoAtividade(userId, tdpf, codigo): #efetiva a anulação (apaga) de uma atividade de um tdpf 
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaAnulacaoAtividade"
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row: #não achou usuário
            conn.close()
            return False, "Usuário Telegram não está registrado no serviço."
        if row[1]!=None:
            conn.close()
            return False, "Usuário Telegram saiu do serviço em "+row[1].strftime('%d/%m/%Y')+"."
        cpf = row[0]
        comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return False, "TDPF não está sendo monitorado para você."
        if row[1]!=None:
            conn.close()
            return False, "O acompanhamento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        if codigo==0:    
            comando = "Select Codigo, TDPF, Data from Atividades Where TDPF=%s Order by Codigo DESC"
            cursor.execute(comando, (tdpf,))
            rows = cursor.fetchall()
        else:
            comando = "Select Codigo, TDPF, Data from Atividades Where Codigo=%s and TDPF=%s"
            cursor.execute(comando, (codigo, tdpf))
            rows = cursor.fetchall()       
            if len(rows)==0:
                conn.close()
                return False, "Atividade (código) inexistente para o TDPF informado."         
    except:
        conn.close()
        return False, "Erro na consulta (efetivaAnulacaoAtividade)."
    if len(rows)==0:
        conn.close()
        return False, "Não há atividade informada para o TDPF." #Não havia data de ciência para o TDPF   
    if codigo == 0:         
        codigo = rows[0][0]    
    try:
        comando = "Delete from Atividades Where Codigo=%s and TDPF=%s"
        cursor.execute(comando, (codigo, tdpf))
        conn.commit() 
        conn.close()  
        return True, ""
    except:
        conn.rollback()
        conn.close()
        return False, "Erro na atualização das tabelas. Tente novamente mais tarde."

def anulaAtividade(update, context): #anula (apaga) a última atividade cadastrada do TDPF
    global pendencias, textoRetorno
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1 and len(parametros)!=2:
        response_message = "Envie o nº do TDPF (16 dígitos, sem espaços) e, opcionalmente, o código da atividade a ser excluída (se não for informado, será excluída a última cadastrada) - separe as informa;óes (TDPF e código) com espaço."
        response_message = response_message+textoRetorno      
    else:
        codigo = 0
        tdpf = getAlgarismos(parametros[0])
        response_message = ""
        if len(parametros)==2:
            try:
                codigo = int(parametros[1])
            except:
                response_message = "Código inválido. Envie o nº do TDPF (16 dígitos, sem espaços) e, opcionalmente, o código da atividade a ser excluída (se não for informado, será excluída a última cadastrada) - separe as informaçóes (TDPF e código) com espaço."
                response_message = response_message+textoRetorno      
        if len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF inválido. Envie o nº do TDPF (16 dígitos, sem espaços) e, opcionalmente, o código da atividade a ser excluída (se não for informado, será excluída a última cadastrada) - separe as informaçóes (TDPF e código) com espaço."
            response_message = response_message+textoRetorno
        if response_message=="":
            efetivou, msgAnulacao = efetivaAnulacaoAtividade(userId, tdpf, codigo)
            eliminaPendencia(userId) #apaga a pendência de informação do usuário                
            if efetivou:
                response_message = "Última atividade ou atividade indicada foi excluída para o TDPF."         
            else:
                response_message = msgAnulacao
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            return
    bot.send_message(userId, text=response_message)  
    return 

            
def efetivaCiencia(userId, tdpf, data): #tenta efetivar a ciência de um tdpf no BD
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaCiencia"
    cursor = conn.cursor(buffered=True)  
    try:
        comando = "Select Encerramento from TDPFS Where Numero=%s"        
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                conn.close()
                return False, "TDPF foi encerrado - monitoramento não é mais necessário ou possível."
        if not achou:    
            conn.close()
            return False, "TDPF não foi localizado - não existe ou foi encerrado há um bom tempo e retirado da base deste serviço."
        comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        achou = False
        if row:
            achou = True
            cpf = row[0]
            if row[1]!=None: #saida
                conn.close()
                return False, "Usuário não está ativo no serviço." #já testado quando acionou o menu
        if not achou:
            conn.close()
            return False, "Usuário não está registrado no serviço." #tb já foi testado
        comando = "Select Desalocacao from Alocacoes Where CPF=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()        
        achou = False
        if row:
            achou = True
            if row[0]!=None:
                conn.close()
                return False, "Usuário não está mais alocado ao TDPF."
        if not achou:
            conn.close()
            return False, "Usuario não está alocado ao TDPF."
        comando = "Select Data from Ciencias Where TDPF=%s and Data>=%s Order by Data DESC"
        cursor.execute(comando, (tdpf, datetime.strptime(data, "%d/%m/%Y")))
        row = cursor.fetchone()
        if row:
            conn.close()
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
        conn.close()
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
        conn.close()
        return True, msg
    except:
        conn.rollback()
        conn.close()
        return False, "Erro ao atualizar as tabelas (efetivaCiencia)."
            

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
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaPrazos"
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
        conn.close()
        return True, "" #retorna se operação foi um sucesso e mensagem de erro em caso de False no primeiro         
    except:
        conn.rollback()
        conn.close()
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
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaAnulacao"
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row: #não achou usuário
            conn.close()
            return False, "Usuário Telegram não está registrado no serviço."
        if row[1]!=None:
            conn.close()
            return False, "Usuário Telegram saiu do serviço em "+row[1].strftime('%d/%m/%Y')+"."
        cpf = row[0]
        comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
        cursor.execute(comando, (cpf, tdpf))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return False, "TDPF não está sendo monitorado para você."
        if row[1]!=None:
            conn.close()
            return False, "O acompanhamento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        comando = "Select Codigo, TDPF, Data from Ciencias Where TDPF=%s Order by Data"
        cursor.execute(comando, (tdpf,))
        rows = cursor.fetchall()
    except:
        conn.close()
        return False, "Erro na consulta (5)."
    if len(rows)==0:
        conn.close()
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
        conn.close() 
        return True, dataAnt #retorna se teve sucesso e a data anterior
    except:
        conn.rollback()
        conn.close()
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
                        if dateTimeObj.date()<datetime.now().date()-timedelta(days=59):
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
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaFinalização"
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, Saida from Usuarios Where idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row: #não achou usuário
            conn.close()
            return False, "Usuário Telegram não está registrado no serviço."
        if row[1]!=None:
            conn.close()
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
                conn.close()
                return False, "TDPF não está sendo monitorado por você. Envie um novo nº de TDPF:"
            else:
                conn.close()
                return True, "Nenhum TDPF está sendo monitorado por você atualmente."
        if row[1]!=None and tdpf!="":
            conn.close()
            return True, "O acompanhamento do TDPF já havia sido finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        elif tdpf!="":
            conn.close()
            chave = row[0]
    except:
        conn.close()
        return False, "Erro na consulta (7). Tente novamente mais tarde."
    try:
        if tdpf!="":
            comando = "Update CadastroTDPFs Set Fim=%s Where Codigo=%s"
            cursor.execute(comando, (datetime.today().date(), chave))
        else:
            comando = "Update CadastroTDPFs Set Fim=%s Where Fiscal=%s"
            cursor.execute(comando, (datetime.today().date(), cpf))
        conn.commit()  
        conn.close()          
        return True, None
    except:
        conn.rollback()
        conn.close()
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
    global pendencias, textoRetorno
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
            conn = conecta()
            if not conn: 
                return 
            comando = "Select Codigo, CPF, email from Usuarios Where Saida Is Null and idTelegram=%s"
            cursor = conn.cursor(buffered=True)
            try:
                cursor.execute(comando, (userId,))
                row = cursor.fetchone()
            except:
                response_message = "Erro na consulta (6)."+textoRetorno
                bot.send_message(userId, text=response_message)  
                conn.close()
                return                
            if not row:
                response_message = "Usuário não registrado ou inativo no serviço."
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)
                conn.close()
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
                    conn.close()
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
                    conn.close()
                    return 
                except:
                    conn.close()
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
    #menu = [['Informa Data de Ciência Relativa a TDPF', 'Anula Ciência Relativa a TDPF'], 
    #        ['Mostra TDPFs Monitorados', 'Mostra TDPFs Supervisionados'],
    #        ['Monitora TDPF(s)', 'Finaliza Monitoramento de TDPF'], 
    #        ['Menu Principal']]
    menu = [['Espontaneidade e Atividades Relativas a TDPF'],
            ['Mostra TDPFs Monitorados', 'Mostra TDPFs Supervisionados'],
            ['Monitora TDPF(s)', 'Finaliza Monitoramento de TDPF'], 
            ['Menu Principal']]
    #userId = update.effective_user.id  
    #bot = update.effective_user.bot     
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu TDPF:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))  
    return  

def mostraMenuCienciasAtividades(update, context):
    global pendencias
    menu = [['Informa Data de Ciência', 'Anula Data de Ciência Informada'], 
            ['Informa Atividade e Prazo', 'Anula Atividade Informada'],
            ['Menu Principal']]   
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu Espontaneidade e Atividades:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))  
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
    conn = conecta()
    if not conn: 
        return "Erro na conexão"
    cursor = conn.cursor(buffered=True)
    #comando = "Select CPF, Saida from Usuarios Where idTelegram=?"
    comando = "Select CPF, Saida from Usuarios Where idTelegram="+str(userId)
    #cursor.execute(comando, float(userId))
    try:
        #cursor.execute(comando, float(userId))
        cursor.execute(comando)
        row = cursor.fetchone()
        conn.close()
        if not row:
            return "Registra Usuário"
        else:
            if row[1]==None:
                return "Desativa Usuário"
            else:
                return "Reativa Usuário"
    except:
        conn.close()
        return "Erro na consulta (tipoOpcao)"            


def opcaoUsuario(update, context): #Cadastra usuário
    global pendencias
    
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    msgOpcao1 = tipoOpcao1(userId)
    conn = None
    if msgOpcao1=="Registra Usuário" or msgOpcao1=="Reativa Usuário":       
        pendencias[userId] = 'registra' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, seu CPF e o código de registro (chave) (separe as informações com espaço):"  
        bot.send_message(userId, text=response_message)
    else:
        conn = conecta()
        if not conn: 
            response_message = "Erro na conexão (8)."            
            bot.send_message(userId, text=response_message)      
            return             
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
    if conn:
        conn.close()                                                    
    return

def verificaUsuario(userId, bot): #verifica se o usuário está cadastrado e ativo no serviço
    global textoRetorno

    conn = conecta()
    if not conn:
        response_message = "Erro na conexão (7)"
        bot.send_message(userId, text=response_message)
        return False
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF from Usuarios Where Saida Is Null and idTelegram=%s"
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            bot.send_message(userId, text="Usuário não está registrado no serviço ou está inativo. "+textoRetorno)  
            return False
        else:
            return True
    except:
        conn.close()
        bot.send_message(userId, text="Erro na consulta (7).")        
        return False                

def opcaoInformaAtividade(update, context): #informa atividade relativa a TDPF e data de vencimento ou prazo em dias
    global pendencias    
    userId = update.effective_user.id  
    bot = update.effective_user.bot      
    eliminaPendencia(userId)     
    achou = verificaUsuario(userId, bot)       
    if achou:              
        pendencias[userId] = 'atividade' #usuário agora tem uma pendência de informação (atividade)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, o nº do TDPF (16 dígitos) e a data de vencimento (dd/mm/aaaa) ou o prazo de vencimento em dias - separe as informações com espaço:"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context) 
    return    

def opcaoInformaCiencia(update, context): #Informa ciência de TDPF
    global pendencias
    userId = update.effective_user.id  
    bot = update.effective_user.bot      
    eliminaPendencia(userId)     
    achou = verificaUsuario(userId, bot)       
    if achou:              
        pendencias[userId] = 'ciencia' #usuário agora tem uma pendência de informação (ciência)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, o nº do TDPF (16 dígitos) e a data de ciência (dd/mm/aaaa) válida para fins de perda da espontaneidade tributária relativa ao respectivo procedimento fiscal - separe as informações com espaço:"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context) 
    return
        
def opcaoPrazos(update, context): #Informa prazos para receber avisos
    global pendencias, textoRetorno  
    userId = update.effective_user.id 
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    comando = "Select d1, d2, d3, Saida from Usuarios Where idTelegram=%s"
    saida = None
    try:
        conn = conecta()
        if not conn:
            response_message = "Erro na conexão (5)"
            bot.send_message(userId, text=response_message)
            return 
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
        conn.close()
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
    conn.close()  
    return


def opcaoAnulaAtividade(update, context): #anula informação de atividade
    global pendencias    
    userId = update.effective_user.id  
    bot = update.effective_user.bot    
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:            
        pendencias[userId] = 'anulaAtividade'  #usuário agora tem uma pendência de informação   
        response_message = "Envie /menu para ver o menu principal. Envie o nº do TDPF (16 dígitos, sem espaços) e, opcionalmente, o código da atividade a ser excluída (se não for informado, será excluída a última cadastrada) - separe as informa;óes (TDPF e código) com espaço."
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context)         
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
    conn = conecta()
    if not conn:
        return  ["Erro na conexão"]
    try:
        cursor = conn.cursor(buffered=True)
        comando = "Select CPF from Usuarios Where idTelegram=%s and Saida Is Null"
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        cpf = row[0]
        if tipo==1:
        #seleciona monitoramentos ativos (incluído pelo usuário, ainda alocado nele e não encerrado)
            comando = '''Select CadastroTDPFs.TDPF as tdpf, TDPFS.Vencimento as Vencimento, Supervisor from CadastroTDPFs, Alocacoes, 
                        TDPFS Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.Fim Is Null and CadastroTDPFs.Fiscal=Alocacoes.CPF and 
                        CadastroTDPFs.TDPF=Alocacoes.TDPF and CadastroTDPFs.TDPF=TDPFS.Numero and Alocacoes.Desalocacao Is Null and 
                        TDPFS.Encerramento Is Null'''
        elif tipo==2:
        #seleciona todos os TDPFs dos quais o usuário é supervisor
            comando = '''Select Alocacoes.TDPF as tdpf, Vencimento from Alocacoes, TDPFS Where Desalocacao Is Null and Encerramento Is Null 
                        and CPF=%s and TDPF=Numero and Supervisor='S' Order by TDPF'''
        else:
            conn.close()
            return None
        cursor.execute(comando, (cpf,))
        listaAux = cursor.fetchall()
        if not listaAux:
            conn.close()
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
            #logging.info(comando)
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
            tdpfForm = formataTDPF(tdpf)
            if cienciaReg: 
                ciencia = cienciaReg[0] #obtem a data de ciência mais recente
            else:
                ciencia = None
            atividades = []
            comando = "Select Codigo, Atividade, Data from Atividades Where TDPF=%s and Data>=%s order by Data"   #somente as atividade que vencem hj ou no futuro são selecionadas
            cursor.execute(comando, (tdpf, datetime.now().date()))
            regAtividades = cursor.fetchall()
            for regAtividade in regAtividades:
                lista = []
                lista.append(regAtividade[0])
                lista.append(regAtividade[1])
                lista.append(regAtividade[2])
                atividades.append(lista)
            if tipo==1:
                registro = [tdpfForm, linha[2], ciencia, vctoTDPF, atividades]
            else:
                registro = [tdpfForm, monitorado, ciencia, vctoTDPF]
            result.append(registro)       
        if len(result)>0:
            logging.info(result)
            conn.close()
            return result
        else:
            conn.close()
            return None
    except:
        conn.close()
        return ["Erro na consulta (6). Tente novamente mais tarde."]
            
        
def opcaoMostraTDPFs(update, context): #Relação de TDPFs e prazos
    global pendencias    
    userId = update.effective_user.id
    bot = update.effective_user.bot      
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        return        
    lista = montaListaTDPFs(userId, 1) #relaciona TDPFs para um usuário comum (NÃO na qualidade de supervisor)
    atividades = []
    if lista==None:
        response_message = "Você não monitora nenhum TDPF ou nenhum deles possui data de ciência informada neste serviço."        
    else:
        if len(lista)==1 and type(lista[0]) is str: #só retornou uma mensagem de erro
            response_message = lista[0]
            bot.send_message(userId, text=response_message) 
            return
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
            logging.info(item[4])
            atividades.append([tdpf, item[4]]) #item[4] é uma lista de atividades
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
        response_message = ""         
        msg = ""
        for atividade in atividades:
            logging.info(atividade)
            for registro in atividade[1]:
                msg = msg + "\na) "+atividade[0]+"; b) "+str(registro[0])+"; c) "+registro[1]+"; d) "+registro[2].strftime('%d/%m/%Y')
        if msg!="":
            response_message = "Lista de atividades dos TDPFs Monitorados:\na)TDPF; b) Código; c) Descrição; d) Vencimento" + msg    
    if response_message!="":        
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
        if len(lista)==1 and type(lista[0]) is str:
            response_message = lista[0]
            bot.send_message(userId, text=response_message) 
            return        
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
    global pendencias
    
    userId = update.effective_user.id
    bot = update.effective_user.bot       
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        mostraMenuPrincipal(update, context)         
        return     
    conn = conecta()
    if not conn:
        bot.send_message(userId, text="Erro na conexão - opcaoEmail")
        return        
    cursor = conn.cursor(buffered=True)
    comando = "Select CPF, email from Usuarios Where Saida Is Null and idTelegram=%s" 
    try:
        cursor.execute(comando, (userId,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            bot.send_message(userId, text="Usuário não está registrado no serviço ou está inativo. "+textoRetorno)
            return
        email = row[1]
    except:
        bot.send_message(userId, text="Erro na consulta (7).")  
        conn.close()      
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
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Espontaneidade e Atividades Relativas a TDPF'), mostraMenuCienciasAtividades))    
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Prazos Para Receber Avisos'), opcaoPrazos))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Registra Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Desativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Reativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Cadastra/Exclui e-Mail'), opcaoEMail))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Data de Ciência'), opcaoInformaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Anula Data de Ciência Informada'), opcaoAnulaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Atividade e Prazo'), opcaoInformaAtividade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Anula Atividade Informada'), opcaoAnulaAtividade))
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
    global updater, termina

    conn = conecta()
    if not conn:
        return
    logging.info("Acionado o disparo de mensagens - "+datetime.now().strftime('%d/%m/%Y %H:%M'))
    cursor = conn.cursor()
    dataAtual = datetime.today().date()
    cursor.execute('Select Mensagem from MensagensCofis Where Data=%s', (dataAtual,))    
    mensagens = cursor.fetchall()
    msgCofis = ""
    for mensagem in mensagens:
        if msgCofis!="":
            msgCofis = msgCofis+";\n"
        msgCofis = msgCofis+mensagem[0]
    if msgCofis!="":
        msgCofis = "Mensagens Cofis:\n"+msgCofis+"."    
    comando = "Select idTelegram, CPF, d1, d2, d3 from Usuarios Where Saida Is Null"
    cursor.execute(comando)
    usuarios = cursor.fetchall()
    totalMsg = 0
    msgDisparadas = 0
    tdpfsAvisadosUpdate = set()
    tdpfsAvisadosInsert = set()
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
        listaUsuario = "" #lista de TDPF com espontaneidade, atividade ou o próprio TDPF vencendo
        cpf = usuario[1]
        d1 = usuario[2]
        d2 = usuario[3]
        d3 = usuario[4]
        #selecionamos os TDPFs do usuário em andamento e monitorados (ativos) pelo serviço
        comando = """
                Select CadastroTDPFs.TDPF as tdpf
                from CadastroTDPFs, TDPFS, Alocacoes
                Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.TDPF=TDPFS.Numero and 
                CadastroTDPFs.TDPF=Alocacoes.TDPF and  
                CadastroTDPFs.Fiscal=Alocacoes.CPF and TDPFS.Encerramento Is Null and 
                CadastroTDPFs.Fim Is Null and Alocacoes.Desalocacao Is Null
                Order by CadastroTDPFs.TDPF 
                """
        cursor.execute(comando, (cpf,))        
        fiscalizacoes = cursor.fetchall()
        comandoCiencias = "Select Data from Ciencias Where TDPF=%s Order By Data DESC"
        comandoAtividades = "Select Atividade, Data from Atividades Where TDPF=%s and Data>=%s Order by Data"
        if fiscalizacoes:
            for fiscalizacao in fiscalizacoes: #percorremos os TDPFs MONITORADOS do usuário
                if termina: #foi solicitado o término do bot
                    return            
                tdpf = fiscalizacao[0] 
                cursor.execute(comandoCiencias, (tdpf,)) #buscamos as ciências do TDPF
                ciencias = cursor.fetchall() #buscamos todas por questões técnicas do mysqlconnector
                if len(ciencias)>0:       
                    dataCiencia = ciencias[0][0].date()  #só é necessária a última (selecionamos por ordem descendente)      
                    prazoRestante = (dataCiencia+timedelta(days=60)-dataAtual).days                
                    if prazoRestante==d1 or prazoRestante==d2 or prazoRestante==d3:
                        if len(listaUsuario)==0:
                            listaUsuario = "Alertas do dia (TDPF | Dias*):"
                        listaUsuario = listaUsuario+"\n"+formataTDPF(tdpf)+ " | "+str(prazoRestante)+" (a)"
           
                #buscamos as atividades do TDPF    
                cursor.execute(comandoAtividades, (tdpf, dataAtual))
                atividades = cursor.fetchall()
                for atividade in atividades:
                    prazoRestante = (atividade[1].date()-dataAtual).days
                    if prazoRestante==0 or prazoRestante==d3: #para atividade, alertamos só no d3 (o menor) e no dia do vencimento (prazo restante == 0)
                        if len(listaUsuario)==0:
                            listaUsuario = "Alertas do dia (TDPF | Dias*):"
                        listaUsuario = listaUsuario+"\n"+formataTDPF(tdpf)+" | "+str(prazoRestante)+" (b)"
                        listaUsuario = listaUsuario+"\nAtividade: "+atividade[0]

        #selecionamos as datas de vencimento dos TDPFs em que o usuário está alocado, mesmo que não monitorados
        comando = """
                Select TDPFS.Numero as tdpf, TDPFS.Vencimento as vencimento from TDPFS, Alocacoes
                Where Alocacoes.CPF=%s and TDPFS.Numero=Alocacoes.TDPF and TDPFS.Encerramento Is Null and 
                Alocacoes.Desalocacao Is Null
                """        
        cursor.execute(comando, (cpf,))     
        comandoVencimento = "Select Codigo, Data from AvisosVencimento Where TDPF=%s and CPF=%s"                   
        tdpfUsuarios = cursor.fetchall()
        for tdpfUsuario in tdpfUsuarios: #percorremos os TDPFs do usuário para ver suas datas de vencimento (TODOS em andamento no qual o usuário esteja atualmente alocado)
            vencimento = tdpfUsuario[1]
            tdpf = tdpfUsuario[0]
            if vencimento: #não deve ser nulo, mas garantimos ...
                vencimento = vencimento.date()
                prazoVenctoTDPF = (vencimento-dataAtual).days
                if not (1<=prazoVenctoTDPF<=15): #se não estiver próximo do vencimento, prosseguimos (pulamos)
                    continue 
                cursor.execute(comandoVencimento, (tdpf, cpf))  #buscamos as datas de aviso para o CPF e TDPF  
                avisos = cursor.fetchall()
                if len(avisos)==0: #não há avisos para o tdpf (este cpf)
                    avisou = None
                else:
                    avisou = avisos[0][1].date() #só retorna uma linha para cada tdpf/cpf      
                    codigo = avisos[0][0] #chave primária do registro        
                #não avisamos do vencimento de TDPF recentemente avisado (prazo: 7 dias - depende da carga)
                if not avisou:
                    podeAvisar = True
                    tdpfsAvisadosInsert.add(tdpf+cpf)
                else:
                    if (avisou+timedelta(days=7))<dataAtual: #vai depender da periodicidade da extração e carga - aqui estamos considerando 7 dias
                        podeAvisar = True
                        tdpfsAvisadosUpdate.add(codigo)                        
                    else:
                        podeAvisar = False                      
                if podeAvisar: #verificar este prazos quando for colocar em produção
                    if len(listaUsuario)==0:
                        listaUsuario = "Alertas do dia (TDPF | Dias*):"
                    listaUsuario = listaUsuario+"\n"+formataTDPF(tdpf)+ " | "+str(prazoVenctoTDPF)+" (c)"                 

        if len(listaUsuario)>0 or msgCofis!="":
            if len(listaUsuario)>0:
                listaUsuario = listaUsuario+"\n*Dias restantes"
                listaUsuario = listaUsuario+"\n(a) P/ recuperação da espontaneidade tributária."
                listaUsuario = listaUsuario+"\n(b) P/ vencimento da Atividade."            
                listaUsuario = listaUsuario+"\n(c) P/ vencimento do TDPF no Ação Fiscal."
            if msgCofis!="":
                if len(listaUsuario)>0:
                    listaUsuario = listaUsuario+"\n\n"
                listaUsuario = listaUsuario+msgCofis
            logging.info("Disparando mensagem para "+cpf)
            updater.bot.send_message(usuario[0], text=listaUsuario)   
            #logging.info(listaUsuario)            
            totalMsg+=1
            msgDisparadas+=1
            if msgDisparadas>=30:
                msgDisparadas = 0
                time.sleep(1) #a cada 30 mensagens, dormimos um segundo (limitação do Bot é 30 por seg - TESTE) 

    logging.info("Total de mensagens disparadas: "+str(totalMsg))        
    #se avisamos do vencimento do TDPF, colocamos a data na tabela para não avisarmos novamente
    #todo dia (espera 7 dias)            
    lista = []
    for tdpfCpf in tdpfsAvisadosInsert:
        tupla = (tdpfCpf[:16], tdpfCpf[16:], dataAtual)
        print(tupla)
        lista.append(tupla)
    if len(lista)>0:
        logging.info("Inserção:")
        logging.info(lista)
        comando = "Insert Into AvisosVencimento (TDPF, CPF, Data) Values (%s, %s, %s)"
        try:
            cursor.executemany(comando, lista)    
            conn.commit()
        except:
            logging.info("Erro ao tentar inserir as datas de aviso na tabela AvisosVencimento.")
            conn.rollback()   
    lista = []
    for codigo in tdpfsAvisadosUpdate:
        tupla = (dataAtual, codigo)
        lista.append(tupla)                         
    if len(lista)>0:   
        logging.info("Atualização:")
        logging.info(lista)         
        comando = "Update AvisosVencimento Set Data=%s Where Codigo=%s" 
        try:
            cursor.executemany(comando, lista)
            conn.commit()
        except:
            logging.info("Erro ao tentar atualizar as datas de aviso na tabela AvisosVencimento.")
            conn.rollback()
    return


def disparador():
    global termina, dirLog, sistema
    while not termina:
        schedule.run_pending() 
        time.sleep(60) #remover isso e 'descomentar' as linhas abaixo
        #time.sleep(24*60*60) #dorme por 24 horas até verificar se deve fazer o disparo de mensagens 
        #a cada 24h, inicia um arquivo de log diferente
        #logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Bot'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO, force=True)       
    return 

def conecta():
    global MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, hostSrv

    try:
        logging.info(MYSQL_DATABASE)
        #logging.info(MYSQL_PASSWORD)
        logging.info(MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv,
                                    database=MYSQL_DATABASE)
        return conn                         
    except mysql.connector.Error as err:
        print("Erro na conexão com o BD - veja Log")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Usuário ou senha inválido(s).")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Banco de dados não existe.")
        else:
            logging.error(err)
            logging.error("Erro na conexão com o Banco de Dados")
        return None


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
    print("Token não informado")
    sys.exit(1)

logging.info("Conectando ao servidor de banco de dados ...")
conn = conecta() #não será utilizada - apenas conectamos para ver se está ok
if not conn:
    sys.exit(1)
logging.info("Conexão efetuada com sucesso ao MySql!")
conn.close() #cada função criará sua conexão (aqui é só um teste para inicializarmos o Bot)
#dias de antecedência padrão para avisar
d1padrao = 30
d2padrao = 20
d3padrao = 5

atividadeTxt = {} #guarda o tdpf e o prazo de uma atividade pendente de informação do texto de sua descrição (id: [tdpf, data])

pendencias = {} #indica que próxima função deve ser chamada para analisar entrada de dados

#encaminha a pendência para a função adequada para tratar o input do usuário
dispatch = { 'registra': registra, 'ciencia': ciencia, 'prazos': prazos, 'acompanha': acompanha,
             'anulaCiencia': anulaCiencia, 'fim': fim, 'email': cadastraEMail,
             'atividade': atividade, 'anulaAtividade': anulaAtividade, 'atividadeTexto': atividadeTexto}
textoRetorno = "\nEnvie /menu para retornar ao menu principal"
updater = None #para ser acessível ao disparador de mensagens
#schedule.every().day.at("07:30").do(disparaMensagens)
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