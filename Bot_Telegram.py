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
from telegram.ext import CommandHandler, Filters, MessageHandler #,CallbackQueryHandler
from telegram import ReplyKeyboardMarkup #,InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton 

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
import smtplib

from openpyxl import Workbook
#from openpyxl.styles import colors
from openpyxl.styles import Font #, Color
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
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

def limpaMarkdown(texto):
    return texto.replace(".", "\.").replace("_", "\_").replace("[", "\[").replace("]", "\]").replace(")", "\)").replace("(", "\(").replace("-","\-").replace("|","\|")#.replace("*", "\\*")        

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

def enviaEmail(email, texto, assunto, arquivo=None):
    try:
        server = smtplib.SMTP('INETRFOC.RFOC.SRF: 25') #servidor de email Notes
        pass
    except:
        return 1	
    # create message object instance
    msg = MIMEMultipart()
    # setup the parameters of the message
    msg['From'] = "botespontaneidade@rfb.gov.br"
    msg['To'] = email
    msg['Subject'] = assunto
    # add in the message body
    msg.attach(MIMEText(texto, 'plain'))  
    if arquivo!=None and arquivo!="":  
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(arquivo, "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename='+arquivo)
        msg.attach(part)                 
    # send the message via the server.
    try:
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        pass
    except:
        return 2
    server.quit()  
    return 3 #sucesso

def verificaUsuarioTelegram(conn, userId): #retorna True, CPF e "" se está cadastrado e ativo; False CPF/vazio e mensagem se não está no serviço ou está inativo (saiu) 
    if conn==None:
        return False, "", ""
    if userId==None or userId==0:
        return False, "", "userId inválido"
    comando = "Select CPF, Adesao, Saida from Usuarios Where idTelegram=%s"
    cursor = conn.cursor(buffered=True)
    cursor.execute(comando, (userId,))
    row = cursor.fetchone()
    if row:
        cpf = row[0]
        if row[1]==None: #adesão
            return False, cpf, "Usuário não se registrou no serviço"
        if row[2]!=None: #saída
            return False, cpf, "Usuário está INATIVO no serviço - saída em "+row[2].strftime("%d/%m/%Y") 
        return True, cpf, ""
    else:
        return False, "", "Usuário não está na base de dados"    

def verificaMonitoramento(conn, cpf, tdpf): #verifica se o usuário está monitorando o TDPF; retorna False, vazio, vazio e mensagem se não, 
                                            #e True, chaveTdpf, chaveFiscal e vazio caso afirmativo
    cursor = conn.cursor(buffered=True)
    comando = """Select CadastroTDPFs.Codigo, CadastroTDPFs.Fim, CadastroTDPFs.TDPF, Fiscais.Codigo from CadastroTDPFs, Fiscais, TDPFS 
                 Where Fiscais.CPF=%s and Fiscais.Codigo=CadastroTDPFs.Fiscal and CadastroTDPFs.TDPF=TDPFS.Codigo and TDPFS.Numero=%s"""
    cursor.execute(comando, (cpf, tdpf))
    row = cursor.fetchone()
    if not row:
        return False, None, None, "TDPF não está sendo monitorado pelo usuário."
    if row[1]!=None:
        return False, None, None, "O monitoramento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."
    chaveTdpf = row[2]  
    chaveFiscal = row[3]  
    return True, chaveTdpf, chaveFiscal, ""    

def verificaAlocacao(conn, cpf, tdpf): #verifica se o usuário está alocado ao TDPF e se o TDPF está em andamento; 
                                       #retorna False, vazio, vazio e mensagem se não (ou TDPF inexistente), e True, chaveTdpf, chaveFiscal e vazio caso afirmativo
    cursor = conn.cursor(buffered=True)
    comando = """Select TDPFS.Numero, TDPFS.Codigo, Fiscais.Codigo from Alocacoes, TDPFS, Fiscais 
                 Where Fiscais.CPF=%s and Fiscais.Codigo=Alocacoes.Fiscal and TDPFS.Numero=%s and Alocacoes.TDPF=TDPFS.Codigo 
                 and TDPFS.Encerramento Is Null and Alocacoes.Desalocacao Is Null"""
    cursor.execute(comando, (cpf, tdpf))        
    row = cursor.fetchone()
    if not row:
        return False, None, None, "TDPF inexistente/encerrado ou usuário não está alocado a ele ou foi desalocado."
    chaveTdpf = row[1]    
    chaveFiscal = row[2]
    return True, chaveTdpf, chaveFiscal, ""

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
        cursor.execute("Select Codigo, CPF, Chave, Adesao, ValidadeChave, Tentativas from Usuarios where CPF=%s", (cpf,))  
        row = cursor.fetchone()
        if row:
            codigo = row[0]
            validade = row[4]
            tentativas = row[5]   
            if tentativas==None:
                tentativas = 0   
            if validade==None:
                validade = datetime.strptime("31/12/2050", "%d/%m/%Y")                    
            if validade.date()<datetime.today().date():
                eliminaPendencia(userId)                
                bot.send_message(userId, text="Validade da chave está expirada.") 
                mostraMenuPrincipal(update, context)  
                conn.close()  
                return   
            if tentativas>3:
                eliminaPendencia(userId)                               
                bot.send_message(userId, text="Número de tentativas excedida.") 
                mostraMenuPrincipal(update, context)  
                conn.close()  
                return                                   
            if row[2]==chave and chave>=100000: #chave tem que ser igual à registrada e ter 6 ou mais dígitos (atualmente, 6)
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
                    response_message = "Erro ao registrar o usuário no serviço. Tente novamente mais tarde. Se o erro persistir, contacte o suporte."  
                bot.send_message(userId, text=response_message) 
                mostraMenuPrincipal(update, context)  
                conn.close()  
                return
            else:
                try:
                    comando =  "Update Usuarios set Tentativas=%s Where Codigo=%s"
                    cursor.execute(comando, ((tentativas+1), codigo))   
                    conn.commit()  
                except:
                    conn.rollback()
                    logging.info('Erro na atualização de tentativas - registro '+cpf)
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

def envioChave(update, context): #envia a chave de registro para o e-mail do usuário
    global pendencias, textoRetorno, ambiente 
    response_message = ""
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o CPF do usuário."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)        
    else:
        cpf = getAlgarismos(parametros[0])                   
        logging.info('Envio chave '+cpf)
        if not validaCPF(cpf):
            response_message = "CPF inválido. Envie novamente o CPF do usuário."
            response_message = response_message+textoRetorno
            bot.send_message(userId, text=response_message)
            return 
        eliminaPendencia(userId)             
        conn = conecta()                       
        if not conn:   
            bot.send_message(userId, text="Erro na conexão - envioChave")  
            mostraMenuPrincipal(update, context)                    
            return
        cursor = conn.cursor(buffered=True)  
        cursor.execute("Select Codigo, CPF, email, DataEnvio from Usuarios where CPF=%s", (cpf,))  
        row = cursor.fetchone()
        if row:
            codigo = row[0]
            email = row[2]
            erro = False
            if email==None or email=='':
                bot.send_message(userId, text="CPF não tem e-mail associado na base de dados. Contacte o suporte ou cadastre um na opção 'Cadastros -> Cadastra/Exclui e-Mail' se você já estiver registrado no serviço.")  
                erro = True             
            elif not verificaEMail(email):
                bot.send_message(userId, text="O e-mail do usuário é inválido. Contacte o suporte ou exclua o atual e cadastre um novo na opção 'Cadastros -> Cadastra/Exclui e-Mail' se você já estiver registrado no serviço.")
                erro = True
            elif not ("@rfb.gov.br" in email):
                bot.send_message(userId, text="O e-mail do usuário não é institucional. Contacte o suporte ou exclua o atual e cadastre um novo na opção 'Cadastros -> Cadastra/Exclui e-Mail' se você já estiver registrado no serviço.")
                erro = True
            if erro:    
                conn.close()                 
                mostraMenuPrincipal(update, context)                    
                return                 
            dataEnvio = row[3]
            if dataEnvio==None:
                dataEnvio = datetime.strptime("01/01/1900", "%d/%m/%Y")
            dataEnvio = dataEnvio.date()    
            if dataEnvio<datetime.today().date():
                chave = randint(100000, 1000000) #a chave é um número inteiro de seis dígitos
                message = "Prezado(a),\n\nSua chave SIGILOSA de registro no Bot Espontaneidade (Telegram) é "+str(chave)+"\n\nEsta chave é utilizada também para acesso via ContÁgil no Script AlertasFiscalização e tem validade de 30 dias.\n\nAtenciosamente,\n\nDisav/Cofis\n\nAmbiente: "+ambiente 
                assunto = "Chave de Registro - Bot Espontaneidade"
                sucesso = enviaEmail(email, message, assunto)
                if sucesso==3:
                    comando = "Update Usuarios Set Chave=%s, ValidadeChave=%s, Tentativas=%s, DataEnvio=%s Where Codigo=%s"
                    validade = datetime.today().date()+timedelta(days=30) #a chave tem validade de 30 dias
                    try:
                        cursor.execute(comando, (chave, validade, 0, datetime.today().date(), codigo))
                        conn.commit()
                        bot.send_message(userId, text="Chave foi enviada para o e-mail institucional do usuário.")                         
                    except:
                        conn.rollback()    
                        bot.send_message(userId, text="Erro ao inserir a chave na tabela. A chave enviada não será reconhecida. Tente novamente mais tarde. Se o erro persistir, contacte o suporte.")  
                else:
                    bot.send_message(userId, text="Houve erro no envio do e-mail. Tente novamente mais tarde. Se o erro persistir, contacte o suporte. "+str(sucesso))                                            
            else:
                bot.send_message(userId, text="A chave já foi enviada hoje - é vedado o reenvio no mesmo dia.")       
        else:
            bot.send_message(userId, text="CPF do usuário não foi encontrado na base de dados.")      
        mostraMenuPrincipal(update, context) 
        conn.close()    
    return
                

def acompanha(update, context): #inicia o monitoramente de um ou de TODOS os TDPFs que o usuário esteja alocado
    global pendencias, textoRetorno
    bot = update.effective_user.bot
    userId = update.message.from_user.id      
    conn = conecta()
    if not conn: 
        response_message = "Erro na conexão - acompanha."
        bot.send_message(userId, text=response_message)
        eliminaPendencia(userId)
        mostraMenuPrincipal(update, context)
        return    
    cursor = conn.cursor(buffered=True)     
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Número de informações (parâmetros) inválido. Envie somente o nº do TDPF ou a palavra TODOS."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)
        conn.close()
        return        
    else:  
        tdpfs = None
        bAlocacao = False
        bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
        if not bUsuario:
            response_message = msg
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            eliminaPendencia(userId) #apaga a pendência de informação do usuário
            conn.close()
            return                          
        info = parametros[0]
        if info.upper().strip() in ["TODOS", "TODAS"]:
            comando = """Select TDPFS.Numero, TDPFS.Codigo, Fiscais.Codigo from Alocacoes, TDPFS, Fiscais 
                         Where Alocacoes.Desalocacao Is Null and Fiscais.CPF=%s and Alocacoes.Fiscal=Fiscais.Codigo and 
                         Alocacoes.TDPF=TDPFS.Codigo and TDPFS.Encerramento Is Null"""
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
            bAlocacao, chaveTdpf, chaveFiscal, msg = verificaAlocacao(conn, cpf, tdpf)                 
            if not bAlocacao:
                response_message = msg
        if not tdpfs and not bAlocacao:
            eliminaPendencia(userId) #apaga a pendência de informação do usuário            
            bot.send_message(userId, text=response_message) 
            mostraMenuPrincipal(update, context)
            conn.close()
            return
        atualizou = False 
        try:
            for tdpfObj in tdpfs:
                chaveTdpf = tdpfObj[1]
                chaveFiscal = tdpfObj[2]
                comando = """Select CadastroTDPFs.Codigo, CadastroTDPFs.Fim from CadastroTDPFs
                                Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.TDPF=%s"""
                cursor.execute(comando, (chaveFiscal, chaveTdpf))
                row = cursor.fetchone()
                if row:
                    if row[1]!=None:
                        comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                        cursor.execute(comando, (row[0],))
                        atualizou = True
                else:
                    comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                    cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date()))
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

def efetivaAtividade(userId, tdpf, atividade, vencimento, inicio): #tenta efetivar uma atividade para um certo tdpf no BD
    conn = conecta()
    if not conn:         
        return False, "Erro na conexão - efetivaAtividade"
    cursor = conn.cursor(buffered=True)  
    try:
        bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
        if not bUsuario:
            conn.close()
            return False, msg #tb já foi testado  
        bAlocacao, chaveTdpf, chaveFiscal, msg = verificaAlocacao(conn, cpf, tdpf) 
        if not bAlocacao:
            conn.close()
            return False, msg         
        comando = "Select Codigo, Fim from CadastroTDPFs Where TDPF=%s and CadastroTDPFs.Fiscal=%s"
        cursor.execute(comando, (chaveTdpf, chaveFiscal))
        row = cursor.fetchone()        
        tdpfCadastrado = False
        fim = None
        if row:
            tdpfCadastrado = True  
            chaveCad = row[0]
            fim = row[1]
            #logging.info("TDPF cadastrado")          
    except:
        conn.close()
        return False, "Erro na consulta (efetivaAtividade)."
    try:
        if isDate(vencimento):
            dataVenc = datetime.strptime(vencimento, "%d/%m/%Y").date()
        else:
            dataVenc = datetime.today().date()+timedelta(days=int(vencimento)) 
        dataInicio = datetime.strptime(inicio, "%d/%m/%Y").date()          
        comando = "Insert into Atividades (TDPF, Atividade, Vencimento, Inicio) Values (%s, %s, %s, %s)"
        cursor.execute(comando, (chaveTdpf, atividade.upper(), dataVenc, dataInicio))
        msg = ""
        if fim!=None:
            msg = " Monitoramento deste TDPF foi reativado."
            comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
            cursor.execute(comando, (chaveCad,))                         
        elif not tdpfCadastrado:
            msg = " Monitoramento deste TDPF foi iniciado."
            comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
            cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date()))
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
        response_message = "Descrição inválida (menos de 3 caracteres). Envie somente a descrição do texto (SEM informação protegida por sigilo) ou 'cancela'(sem aspas) para cancelar."
        response_message = response_message+textoRetorno
    if len(msg)>50:
        response_message = "Descrição inválida (mais de 50 caracteres). Envie somente a descrição do texto (SEM informação protegida por sigilo) ou 'cancela'(sem aspas) para cancelar."
        response_message = response_message+textoRetorno        
    else:
        atividade = msg.upper()
        if atividade=="CANCELA":
            eliminaPendencia(userId)
            mostraMenuPrincipal(update, context)
            return     
        efetivou, msgAtividade = efetivaAtividade(userId, atividadeTxt[userId][0], atividade, atividadeTxt[userId][1], atividadeTxt[userId][2])
        eliminaPendencia(userId) #apaga a pendência de informação do usuário        
        if efetivou:
            response_message = "Atividade registrada para o TDPF. No script do ContÁgil, você poder registrar informações p/ esta atividade sem restrição de sigilo no campo Observações."
            if msgAtividade!=None and msgAtividade!="":
                response_message = response_message+msgAtividade
        else:
            if msgAtividade==None or msgAtividade=='':
                msgAtividade = "Erro ao registrar a atividade. Tente novamente mais tarde."
            response_message = msgAtividade  #como são digitadas duas informações, desprezamos e retornamos ao menu pois o erro pode estar na anterior
        bot.send_message(userId, text=response_message) 
        mostraMenuPrincipal(update, context)
        return            
    bot.send_message(userId, text=response_message)  
    return                

def atividade(update, context): #critica e tenta efetivar a realização de uma atividade com prazo a vencer
    global pendencias, textoRetorno, atividadeTxt
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    msgAtividade = "Envie novamente o TDPF (16 dígitos), a data ou o prazo de vencimento em dias e a data de início da atividade."
    parametros = getParametros(msg)
    if len(parametros)!=3:
        response_message = "Número de informações (parâmetros) inválido. "+msgAtividade
        response_message = response_message+textoRetorno
    else:
        response_message = ""
        tdpf = getAlgarismos(parametros[0])
        data = parametros[1]
        inicio = parametros[2]
        if data.isdigit() and len(data)==8:
            data = data[:2]+"/"+data[2:4]+"/"+data[4:]   
        if inicio.isdigit() and len(inicio)==8:
            inicio = inicio[:2]+"/"+inicio[2:4]+"/"+inicio[4:]                      
        if len(tdpf)!=16 or not tdpf.isdigit():
            response_message = "TDPF inválido. "+msgAtividade
            response_message = response_message+textoRetorno                          
        elif not isDate(data) and not data.isdigit():
            response_message = "Data/prazo inválido. "+msgAtividade
            response_message = response_message+textoRetorno             
        if response_message=="" and not isDate(inicio):
            response_message = "Data de início da atividade inválida. "+msgAtividade
            response_message = response_message+textoRetorno 
        if len(response_message)>0:           
            bot.send_message(userId, text=response_message)  
            return                                            
        prazo = 0
        dateTimeObj = None
        if isDate(data):
            try:
                dateTimeObj = datetime.strptime(data, '%d/%m/%Y').date()
                if dateTimeObj<=datetime.now().date():
                    response_message = "Data de vencimento deve ser futura. "+msgAtividade
                    response_message = response_message+textoRetorno                      
            except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
                response_message = "Erro na conversão da data de vencimento. "+msgAtividade
                response_message = response_message+textoRetorno   
        else:
            try:
                prazo = int(data)  
                if prazo>365 or prazo<=0:
                    response_message = "Prazo de vencimento deve ser superior a 0 e inferior a 366 dias. "+msgAtividade
                    response_message = response_message+textoRetorno                          
            except: 
                response_message = "Erro na conversão do prazo. "+msgAtividade
                response_message = response_message+textoRetorno 
        if len(response_message)==0:
            try:
                dataInicio =  datetime.strptime(inicio, '%d/%m/%Y').date()
                if dataInicio>datetime.now().date():
                    response_message = "Data de início deve ser atual ou passada. "+msgAtividade
                    response_message = response_message+textoRetorno                      
            except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
                response_message = "Erro na conversão da data de início. "+msgAtividade
                response_message = response_message+textoRetorno                                                  
        if len(response_message)==0:
            eliminaPendencia(userId)
            pendencias[userId] = 'atividadeTexto'
            atividadeTxt[userId] = [tdpf, data, inicio]
            response_message = "Informe a *descrição da atividade SEM QUALQUER INFORMAÇÃO PROTEGIDA POR SIGILO* (máximo de 50 caracteres):"                    
    bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    return     

def efetivaAnulacaoAtividade(userId, tdpf, codigo): #efetiva a anulação (apaga) de uma atividade de um tdpf (o código é a chave primária da atividade - opcional; se não houver, é o último registro daquele TDPF)
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaAnulacaoAtividade"
    cursor = conn.cursor(buffered=True)
    bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
    if not bUsuario:
        conn.close()
        return False, msg #tb já foi testado     
    try:
        bAlocacao, chaveTdpf, chaveFiscal, msg = verificaAlocacao(conn, cpf, tdpf)
        if not bAlocacao:
            conn.close()
            return False, msg        
        bMonitoramento, chaveTdpf, chaveFiscal, msg = verificaMonitoramento(conn, cpf, tdpf)
        if not bMonitoramento:
            conn.close()
            return False, msg
        if codigo==0:    
            comando = "Select Codigo, TDPF, Inicio from Atividades Where TDPF=%s Order by Codigo DESC"
            cursor.execute(comando, (chaveTdpf,))
            rows = cursor.fetchall()
        else:
            comando = "Select Codigo, TDPF, Inicio from Atividades Where Codigo=%s and TDPF=%s"
            cursor.execute(comando, (codigo, chaveTdpf))
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
        cursor.execute(comando, (codigo, chaveTdpf))
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

def efetivaHorasAtividade(userId, codigo, horas):
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaHorasAtividade"
    cursor = conn.cursor(buffered=True)
    bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
    if not bUsuario:
        conn.close()
        return False, msg #tb já foi testado  
    try:
        comando = """Select Atividades.Atividade, Alocacoes.Desalocacao, Atividades.TDPF, Atividades.Inicio, Fiscais.Codigo
                     From Atividades, Alocacoes, Fiscais, CadastroTDPFs
                     Where Atividades.Codigo=%s and Atividades.TDPF=Alocacoes.TDPF and Fiscais.CPF=%s and Fiscais.Codigo=Alocacoes.Fiscal
                     and CadastroTDPFs.Fiscal=Fiscais.Codigo and CadastroTDPFs.TDPF=Atividades.TDPF and CadastroTDPFs.Fim Is Null"""
        cursor.execute(comando, (codigo, cpf))
        linhas = cursor.fetchall()
    except:
        conn.close()
        return False, "Erro nas consultas - efetivaHorasAtividade"    
    bAchou = True
    if linhas==None:
        bAchou = False
    if len(linhas)==0:
        bAchou = False
    if not bAchou:
        conn.close()
        return False, "Código da atividade não localizado ou usuário não está alocado ao TDPF ou o monitora"        
    linha = linhas[0]
    if linha[1]!=None:
        conn.close()
        return False, "Usuário não está mais alocado ao TDPF"
    chaveTdpf = linha[2]
    chaveFiscal = linha[4]    
    comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
    try:
        cursor.execute(comando, (chaveFiscal, chaveTdpf))
        row = cursor.fetchone()
    except:
        conn.close()
        return False,  "Erro na consulta monitoramento - efetivaHorasAtividade"    
    if not row:
        conn.close()
        return False, "TDPF não está sendo monitorado para você."
    if row[1]!=None:
        conn.close()
        return False, "O acompanhamento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."        
    try:
        comando = "Update Atividades Set Horas=%s Where Codigo=%s"
        cursor.execute(comando, (horas, codigo))   
        conn.commit()
        conn.close() 
        return True, linha[0]
    except:
        conn.rollback()
        conn.close()
        return False, "Erro na atualização da tabela - efetivaHorasAtividade"    

def informaHorasAtividade(update, context): #registra horas gastas em uma atividade
    global pendencias, textoRetorno
    msgHorasAtiv = "Envie o código da atividade e a quantidade de horas dispendidas (número inteiro) até o momento - separe as informações (código e data) com espaço."
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=2:
        response_message = msgHorasAtiv
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return            
    codigo = parametros[0]
    if not codigo.isdigit():
        response_message = "Código inválido. "+msgHorasAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return
    try:
        codigo = int(codigo)
    except:
        response_message = "Código inválido (2). "+msgHorasAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return                  
    horas = parametros[1] 
    if not horas.isdigit():
        response_message = "Quantidade de horas inválida. "+msgHorasAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return
    if len(horas)>3:
        response_message = "Quantidade de horas inválida (2). "+msgHorasAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return         
    try:
        horas = int(horas)
    except:
        response_message = "Quantidade de horas inválida (3). "+msgHorasAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return    
    efetivou, msgAnulacao = efetivaHorasAtividade(userId, codigo, horas)
    eliminaPendencia(userId) #apaga a pendência de informação do usuário                
    if efetivou:
        response_message = "Horas dispendidas na atividade "+msgAnulacao+" foram registradas."         
    else:
        response_message = msgAnulacao
    bot.send_message(userId, text=response_message) 
    mostraMenuPrincipal(update, context)
    return

def efetivaTerminoAtividade(userId, codigo, dataTermino, horas):
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaTerminoAtividade"
    cursor = conn.cursor(buffered=True)
    bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
    if not bUsuario:
        conn.close()
        return False, msg #tb já foi testado      
    try:
        comando = """Select Atividades.Atividade, Alocacoes.Desalocacao, Atividades.TDPF, Atividades.Inicio, Fiscais.Codigo, Atividades.TDPF
                     From Atividades, Alocacoes, Fiscais, CadastroTDPFs
                     Where Atividades.Codigo=%s and Atividades.TDPF=Alocacoes.TDPF and Fiscais.CPF=%s and Alocacoes.Fiscal=Fiscais.Codigo
                     and Atividades.TDPF=CadastroTDPFs.TDPF and Fiscais.Codigo=CadastroTDPFs.Fiscal and CadastroTDPFs.Fim Is Null"""
        cursor.execute(comando, (codigo, cpf))
        linhas = cursor.fetchall()
    except:
        conn.close()
        return False, "Erro nas consultas - efetivaTerminoAtividade"    
    bAchou = True
    if linhas==None:
        bAchou = False
    if len(linhas)==0:
        bAchou = False
    if not bAchou:
        conn.close()
        return False, "Código da atividade não localizado ou usuário não está alocado ao TDPF ou o monitora"        
    linha = linhas[0]
    if linha[1]!=None:
        conn.close()
        return False, "Usuário não está mais alocado ao TDPF"
    chaveFiscal = linha[4]
    chaveTdpf = linha[5]
    dataInicio = linha[3]    
    if dataInicio!=None:
        if dataTermino<dataInicio:
            conn.close()
            return False, "Data de término não pode ser anterior à data de início da atividade ("+dataInicio.strftime("%d/%m/%Y")+")"
    tdpf = linha[2]    
    comando = "Select Codigo, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
    try:
        cursor.execute(comando, (chaveFiscal, chaveTdpf))
        row = cursor.fetchone()
    except:
        conn.close()
        return False,  "Erro na consulta monitoramento - efetivaTerminoAtividade"    
    if not row:
        conn.close()
        return False, "TDPF não está sendo monitorado para você."
    if row[1]!=None:
        conn.close()
        return False, "O acompanhamento do TDPF foi finalizado em "+row[1].strftime('%d/%m/%Y')+"."        
    try:
        comando = "Update Atividades Set Termino=%s, Horas=%s Where Codigo=%s"
        cursor.execute(comando, (dataTermino, horas, codigo))   
        conn.commit()
        conn.close() 
        return True, linha[0]
    except:
        conn.rollback()
        conn.close()
        return False, "Erro na atualização da tabela - efetivaTerminoAtividade"    

def terminoAtividade(update, context): #registra data de término de atividade cadastrada do TDPF
    global pendencias, textoRetorno
    msgTerminoAtiv = "Envie o código da atividade, a data de seu término (dd/mm/yyyy) e a quantidade de horas dispendidas (número inteiro) até o momento - separe as informações (código e data) com espaço."
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=3:
        response_message = msgTerminoAtiv
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return            
    codigo = parametros[0]
    if not codigo.isdigit():
        response_message = "Código inválido. "+msgTerminoAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return
    try:
        codigo = int(codigo)
    except:
        response_message = "Código inválido (2). "+msgTerminoAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return                
    data = parametros[1] 
    if data.isdigit() and len(data)==8:
        data = data[:2]+"/"+data[2:4]+"/"+data[4:]                                     
    if not isDate(data):
        response_message = "Data inválida. "+msgTerminoAtiv
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return                             
    dateTimeObj = None
    try:
        dateTimeObj = datetime.strptime(data, '%d/%m/%Y')
        if dateTimeObj.date()>datetime.now().date():
            response_message = "Data de término não pode ser futura. "+msgTerminoAtiv
            response_message = response_message+textoRetorno   
            bot.send_message(userId, text=response_message) 
            return             
    except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
        response_message = "Erro na conversão da data. "+msgTerminoAtiv
        response_message = response_message+textoRetorno   
        bot.send_message(userId, text=response_message) 
        return  
    horas = parametros[2] 
    if not horas.isdigit():
        response_message = "Quantidade de horas inválida. "+msgTerminoAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return
    if len(horas)>3:
        response_message = "Quantidade de horas inválida (2). "+msgTerminoAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return        
    try:
        horas = int(horas)
    except:
        response_message = "Quantidade de horas inválida (3). "+msgTerminoAtiv
        response_message = response_message+textoRetorno            
        bot.send_message(userId, text=response_message)  
        return    
    efetivou, msgAnulacao = efetivaTerminoAtividade(userId, codigo, dateTimeObj, horas)
    eliminaPendencia(userId) #apaga a pendência de informação do usuário                
    if efetivou:
        response_message = "Término da atividade "+msgAnulacao+" foi registrado."         
    else:
        response_message = msgAnulacao
    bot.send_message(userId, text=response_message) 
    mostraMenuPrincipal(update, context)
    return
            
def efetivaCiencia(userId, tdpf, data, documento): #tenta efetivar a ciência de um tdpf no BD
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaCiencia"
    cursor = conn.cursor(buffered=True)  
    try:
        bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
        if not bUsuario:
            conn.close()
            return False, msg #tb já foi testado  
        bAlocacao, chaveTdpf, chaveFiscal, msg = verificaAlocacao(conn, cpf, tdpf)  
        if not bAlocacao:
            conn.close()
            return False, msg      
        comando = "Select Data from Ciencias Where TDPF=%s and Data>=%s Order by Data DESC"
        cursor.execute(comando, (chaveTdpf, datetime.strptime(data, "%d/%m/%Y")))
        row = cursor.fetchone()
        if row:
            conn.close()
            return False, "Data de ciência informada DEVE ser posterior à ultima informada para o TDPF ("+row[0].strftime('%d/%m/%Y')+")."
        comando = "Select Codigo, Fim from CadastroTDPFs Where TDPF=%s and Fiscal=%s"
        cursor.execute(comando, (chaveTdpf, chaveFiscal))
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
        comando = "Insert into Ciencias (TDPF, Data, Documento) Values (%s, %s, %s)"
        cursor.execute(comando, (chaveTdpf, datetime.strptime(data, "%d/%m/%Y"), documento))
        msg = ""
        if fim!=None:
            msg = " Monitoramento deste TDPF foi reativado."
            comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
            cursor.execute(comando, (chave,))                         
        elif not tdpfCadastrado:
            msg = " Monitoramento deste TDPF foi iniciado."
            comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
            cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date()))
        conn.commit()
        conn.close()
        return True, msg
    except:
        conn.rollback()
        conn.close()
        return False, "Erro ao atualizar as tabelas (efetivaCiencia)."
            
def cienciaTexto(update, context): #obtém a descrição da atividade e chama a função que grava no BD
    global pendencias, textoRetorno, cienciaTxt
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    msg = update.message.text    
    if len(msg)<4:
        response_message = "Descrição inválida (menos de 4 caracteres). Envie somente a descrição do texto ou 'cancela'(sem aspas) para cancelar."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)    
    elif len(msg)>50:
        response_message = "Descrição inválida (mais de 50 caracteres). Envie somente a descrição do texto ou 'cancela'(sem aspas) para cancelar."
        response_message = response_message+textoRetorno
        bot.send_message(userId, text=response_message)                
    else:
        eliminaPendencia(userId) #apaga a pendência de informação do usuário        
        documento = msg.upper().strip()
        if documento=="CANCELA":
            mostraMenuPrincipal(update, context)
            return     
        efetivou, msgEfetivaCiencia= efetivaCiencia(userId, cienciaTxt[userId][0], cienciaTxt[userId][1], documento)
        if efetivou:
            response_message = "Data de ciência registrada para o TDPF."
            if msgEfetivaCiencia!=None and msgEfetivaCiencia!="":
                response_message = response_message+msgEfetivaCiencia
        else:
            response_message = msgEfetivaCiencia         
        bot.send_message(userId, text=response_message) 
        mostraMenuPrincipal(update, context)
    return            

def ciencia(update, context): #critica e tenta efetivar a ciência de um TDPF (registrar data)
    global pendencias, textoRetorno, cienciaTxt
    msgCiencia = "Envie novamente o TDPF e a data de ciência (dd/mm/aaaa) (separados por espaço)."
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
            response_message = "TDPF inválido. "+msgCiencia
            response_message = response_message+textoRetorno            
        elif not isDate(data):
            response_message = "Data inválida. "+msgCiencia
            response_message = response_message+textoRetorno
        else:
            try:
                dateTimeObj = datetime.strptime(data, '%d/%m/%Y')
            except: #não deveria acontecer após o isDate, mas fazemos assim para não correr riscos
                logging.info("Erro na conversão da data "+data+" - UserId "+str(userId))
                response_message = "Erro na conversão da data. "+msgCiencia
                response_message = response_message+textoRetorno   
                bot.send_message(userId, text=response_message)  
                return                
            if dateTimeObj.date()>datetime.now().date():
                response_message = "Data de ciência não pode ser futura. "+msgCiencia
                response_message = response_message+textoRetorno                    
            elif dateTimeObj.date()<datetime.now().date()-timedelta(days=60):
                response_message = "Data de ciência já está vencida para fins de recuperação da espontaneidade tributária. "+msgCiencia
                response_message = response_message+textoRetorno
            else:  
                eliminaPendencia(userId)
                pendencias[userId] = 'cienciaTexto'
                cienciaTxt[userId] = [tdpf, data]
                response_message = "Informe a descrição do documento que efetivou a ciência (ex.: TIPF) (máximo de 50 caracteres):"                                                
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
    bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
    if not bUsuario:
        conn.close()
        return False, msg #tb já foi testado      
    try:
        bMonitoramento, chaveTdpf, chaveFiscal, msg = verificaMonitoramento(conn, cpf, tdpf)  
        if not bMonitoramento:
            conn.close()
            return False, msg      
        bAlocacao, chaveTdpf, chaveFiscal, msg = verificaAlocacao(conn, cpf, tdpf)
        if not bAlocacao:
            conn.close()
            return False, msg              
        comando = "Select Codigo, TDPF, Data from Ciencias Where TDPF=%s Order by Data"
        cursor.execute(comando, (chaveTdpf,))
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

def efetivaFinalizacao(userId, tdpf=""): #finaliza monitoramento de um tdpf ou de todos do usuário (campo Fim da tabela CadastroTDPFs)
    conn = conecta()
    if not conn: 
        return False, "Erro na conexão - efetivaFinalização"
    cursor = conn.cursor(buffered=True)
    bUsuario, cpf, msg = verificaUsuarioTelegram(conn, userId)
    if not bUsuario:
        conn.close()
        return False, msg #tb já foi testado  
    try:
        if tdpf!="":
            comando = """Select CadastroTDPFs.Codigo, Fim, Fiscais.Codigo from CadastroTDPFs, Fiscais, TDPFS 
                         Where Fiscais.CPF=%s and CadastroTDPFs.Fiscal=Fiscais.Codigo and TDPFS.Numero=%s and TDPFS.Codigo=CadastroTDPFs.TDPF"""
            cursor.execute(comando, (cpf, tdpf))            
        else:
            comando = "Select CadastroTDPFs.Codigo, Fim, Fiscais.Codigo from CadastroTDPFs, Fiscais Where Fiscais.CPF=%s and CadastroTDPFs.Fiscal=Fiscais.Codigo and Fim Is Null"
            cursor.execute(comando, (cpf,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            if tdpf!="":
                return False, "TDPF não está sendo monitorado por você. Envie um novo nº de TDPF:"
            else:
                return True, "Nenhum TDPF está sendo monitorado por você atualmente."
        if row[1]!=None and tdpf!="":
            conn.close()
            return True, "O acompanhamento do TDPF já havia sido finalizado em "+row[1].strftime('%d/%m/%Y')+"."
        elif tdpf!="":
            #conn.close()    #<-------VERIFICAR - ACHO QUE ESTÁ ERRADO
            pass
    except:
        conn.close()
        return False, "Erro na consulta (7). Tente novamente mais tarde."
    try:
        chaveReg = row[0]        
        chaveFiscal = row[2]
        if tdpf!="":
            comando = "Update CadastroTDPFs Set Fim=%s Where Codigo=%s"
            cursor.execute(comando, (datetime.today().date(), chaveReg))
        else:
            comando = "Update CadastroTDPFs Set Fim=%s Where Fiscal=%s"
            cursor.execute(comando, (datetime.today().date(), chaveFiscal))
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
    regex1 = '^[a-zA-Z0-9]+[\._]?[a-zA-Z0-9]+[@]\w+[.]\w{2,3}$'
    regex2 = '^[a-zA-Z0-9]+[\._]?[a-zA-Z0-9]+[@]\w+[.]\w+[.]\w{2,3}$'  

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
                eliminaPendencia(userId) #apaga a pendência de informação do usuário                 
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
                        cursor.execute(comando, (email+'@rfb.gov.br', chave))
                    conn.commit()                   
                    if emailAnt!=None and emailAnt!="":
                        if email.upper()=="NULO":
                            response_message = "Email anteriormente informado ("+emailAnt+") foi descadastrado."
                        else:    
                            response_message = "Email anteriormente cadastrado ("+emailAnt+") foi substituído.\n"                
                    if email.upper()!="NULO":
                        response_message = response_message+"Email cadastrado com sucesso - {}@rfb.gov.br.".format(email)    
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)
                    conn.close()
                    return 
                except:
                    conn.close()
                    response_message = "Erro na atualização das tabelas. Tente novamente mais tarde."
                    bot.send_message(userId, text=response_message) 
                    mostraMenuPrincipal(update, context)  
                    return                  
    bot.send_message(userId, text=response_message)  
    return


def start(update, context): #comandos /start /menu /retorna acionam esta opção
    global pendencias
    
    userId = update.message.from_user.id  
    bot = update.effective_user.bot
    logging.info(update.message.from_user.first_name+" - "+str(userId))    
    if update.effective_user.is_bot:
        return #não atendemos bots     
    eliminaPendencia(userId)
    msg1 = 'Este serviço controla os prazos p/ recuperação da espontaneidade, p/ vencimento do TDPF e de atividades cadastradas:\n'
    msg2 = '- Alertas sobre a possível recuperação da espontaneidade de contribuintes, em prazos customizáveis (d1 [maior], d2 e d3 [menor] dias antes).\n'
    msg3 = '- Alertas sobre o vencimento do TDPF em duas datas distintas (separadas por não menos do que 8 dias e quando o vencimento se der em até 15 dias).\n'
    msg4 = '- Alertas sobre o vencimento de atividades cadastradas - em d3 e no dia do vencimento.\n\n'
    msg5 = 'Atualmente, o vencimento do TDPF informado por este serviço poderá ocorrer com alguma antecedência devido ao cálculo baseado apenas na data de distribuição. '
    msg6 = 'Isto será corrigido futuramente.\n\n'
    msg7 = '*Digite a qualquer momento /menu para ver o menu principal e estas observações*, inclusive no caso de ocorrer alguma interrupção do serviço.'
    response_message = msg1+msg2+msg3+msg4+msg5+msg6+msg7
    bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
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
    if update.effective_user.is_bot:
        return #não atendemos bots      
    opcao1 =  tipoOpcao1(userId) 
    bot = update.effective_user.bot     
    if opcao1[:4] == 'Erro':
        bot.send_message(userId, text="Erro na consulta ao seu id")
        return
    menu = [[opcao1], ['Solicita Chave de Registro e do ContÁgil'], ['Prazos Para Receber Avisos'], ['Cadastra/Exclui e-Mail'], ['Menu Principal']] 
    #mensagem = bot.send_message(userId, text="Teste apaga mensagem")
    #time.sleep(5)
    #bot.delete_message(userId, mensagem.message_id)
    #return              
    update.message.reply_text("Menu Cadastro:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True)) 
    return 

def mostraMenuTDPF(update, context):
    global pendencias
    #menu = [['Informa Data de Ciência Relativa a TDPF', 'Anula Ciência Relativa a TDPF'], 
    #        ['Mostra TDPFs Monitorados', 'Mostra TDPFs Supervisionados'],
    #        ['Monitora TDPF(s)', 'Finaliza Monitoramento de TDPF'], 
    #        ['Menu Principal']]
    menu = [['Espontaneidade e Atividades Relativas a TDPF'],
            ['Monitora TDPF(s)', 'Finaliza Monitoramento de TDPF'], 
            ['Mostra TDPFs Monitorados', 'Supervisão'],            
            ['Menu Principal']]
    #userId = update.effective_user.id  
    #bot = update.effective_user.bot     
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu TDPF:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))  
    return  

def mostraMenuSupervisao(update, context):
    global pendencias
    menu = [['Mostra TDPFs Supervisionados'],
            ['Envia Atividades (e-Mail) - Superv.'], 
            ['Recuperação Espontaneidade - Superv.'],
            ['Menu TDPF - Monitoramento'],
            ['Menu Principal']]   
    if update.effective_user.is_bot:
        return #não atendemos bots                
    update.message.reply_text("Menu Supervisão:", reply_markup=ReplyKeyboardMarkup(menu, one_time_keyboard=True))  
    return     

def mostraMenuCienciasAtividades(update, context):
    global pendencias
    menu = [['Informa Data de Ciência', 'Anula Data de Ciência Informada'], 
            ['Informa Ativ. e Prazo', 'Anula Atividade'], 
            ['Informa Horas Atividade', 'Informa Término de Ativ.'], 
            ['Ciências e Atividades - Email', 'Menu TDPF - Monitoramento'],
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
    if update.effective_user.is_bot:
        return #não atendemos bots      
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    msgOpcao1 = tipoOpcao1(userId)
    conn = None
    if msgOpcao1=="Registra Usuário" or msgOpcao1=="Reativa Usuário":       
        pendencias[userId] = 'registra' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, seu *CPF e o código de registro (chave)* (separe as informações com espaço):"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
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

def solicitaChaveRegistro(update, context): #envia chave de registro para o e-mail institucional do AFRFB (um envio a cada 24h)   
    global pendencias    
    userId = update.effective_user.id  
    bot = update.effective_user.bot       
    if update.effective_user.is_bot:
        return #não atendemos bots  
    eliminaPendencia(userId)                          
    pendencias[userId] = 'envioChave' #usuário agora tem uma pendência de informação (atividade)
    response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, o *nº do CPF (11 dígitos)* do usuário (fiscal) p/ o qual a chave será enviada:"  
    bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
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
        try:
            conn.close()
        except:
            pass    
        bot.send_message(userId, text="Erro na consulta (8).")        
        return False                

def opcaoInformaAtividade(update, context): #informa atividade relativa a TDPF e data de vencimento ou prazo em dias
    global pendencias   
    if update.effective_user.is_bot:
        return #não atendemos bots      
    userId = update.effective_user.id  
    bot = update.effective_user.bot  
    eliminaPendencia(userId)             
    achou = verificaUsuario(userId, bot)       
    if achou:                      
        pendencias[userId] = 'atividade' #usuário agora tem uma pendência de informação (atividade)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, o *nº do TDPF (16 dígitos), a data de vencimento (dd/mm/aaaa) ou o prazo de vencimento em dias (contados de hoje) e a data de início da atividade (dd/mm/aaaa)* - separe as informações com espaço:"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    else:
        mostraMenuPrincipal(update, context)
    return    

def exibeAtividadesEmAndamento(bot, userId, conn): #exibe atividades em andamento de TDPFs não encerrados para anulação ou informação do término
    consulta = """
                Select Atividades.Codigo, TDPFS.Numero, Atividades.Atividade, Atividades.Inicio, Atividades.Vencimento, Atividades.Horas
                from Atividades, TDPFS, Alocacoes, Usuarios, Fiscais
                Where Usuarios.idTelegram=%s and Usuarios.CPF=Fiscais.CPF and Fiscais.Codigo=Alocacoes.Fiscal and Alocacoes.Desalocacao Is Null and 
                Alocacoes.TDPF=TDPFS.Codigo and TDPFS.Encerramento Is Null and TDPFS.Codigo=Atividades.TDPF and Atividades.Termino Is Null
                Order by Atividades.Inicio, Atividades.Vencimento
                """    
    cursor = conn.cursor(buffered=True)
    try:
        cursor.execute(consulta, (userId,))
        linhas = cursor.fetchall()
    except:    
        bot.send_message(userId, text="Erro na consulta das atividades em andamento")
        return 0 
    i = 0
    msg = ""
    for linha in linhas:
        i+=1
        msg = msg+"\n"+str(i)+") Código "+str(linha[0])+"; TDPF "+formataTDPF(linha[1])+"; "+linha[2]+"; Início "+datetime.strftime(linha[3], "%d/%m/%Y")+"; Vencimento "+datetime.strftime(linha[4], "%d/%m/%Y")+"; Horas "+str(linha[5])
        if i%15==0:
            bot.send_message(userId, text="Atividades em Andamento:"+msg)
            msg = ""
    if msg!="":
        bot.send_message(userId, text="Atividades em Andamento:"+msg)
    if i==0:
        bot.send_message(userId, text="Usuário não possui atividades cadastradas em andamento para seus TDPFs.")            
    return i       

def opcaoAnulaAtividade(update, context): #anula informação de atividade
    global pendencias  
    if update.effective_user.is_bot:
        return #não atendemos bots         
    userId = update.effective_user.id  
    bot = update.effective_user.bot    
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:    
        conn = conecta()
        if not conn:
            bot.send_message(userId, text="Erro ao tentar conectar ao Banco de Dados - opcaoAnulaAtividade")
            mostraMenuPrincipal(update, context)
            return
        if exibeAtividadesEmAndamento(bot, userId, conn)==0:
            conn.close()
            mostraMenuPrincipal(update, context) 
            return                
        pendencias[userId] = 'anulaAtividade'  #usuário agora tem uma pendência de informação   
        response_message = "Envie /menu para ver o menu principal. Envie o *nº do TDPF (16 dígitos, sem espaços) e, opcionalmente, o código da atividade* a ser excluída (se o código não for informado, será excluída a última cadastrada p/ o TDPF) - separe as informa;óes (TDPF e código) com espaço."
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
        conn.close()
    else:
        mostraMenuPrincipal(update, context)         
    return

#'Informa Término de Atividade'
def opcaoInformaTerminoAtividade(update, context): 
    global pendencias 
    if update.effective_user.is_bot:
        return #não atendemos bots      
    userId = update.effective_user.id  
    bot = update.effective_user.bot 
    eliminaPendencia(userId)             
    achou = verificaUsuario(userId, bot)       
    if achou:    
        conn = conecta()
        if not conn:
            bot.send_message(userId, text="Erro ao tentar conectar ao Banco de Dados - opcaoInformaTerminoAtividade")
            mostraMenuPrincipal(update, context)
            return
        if exibeAtividadesEmAndamento(bot, userId, conn)==0:
            conn.close()
            mostraMenuPrincipal(update, context) 
            return
        pendencias[userId] = 'informaTerminoAtividade' #usuário agora tem uma pendência de informação (término atividade)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, *o código da atividade, a data de seu término (dd/mm/aaaa) e quantidade de horas dispendidas (número inteiro)* até o momento - separe as informações com espaço:"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
        conn.close()
    else:
        mostraMenuPrincipal(update, context)   
    return

#'Informa horas - usuário pode informar horas gastas a qualquer momento e depois ir alterando (exceto após o término)
def opcaoInformaHorasAtividade(update, context): 
    global pendencias 
    if update.effective_user.is_bot:
        return #não atendemos bots      
    userId = update.effective_user.id  
    bot = update.effective_user.bot 
    eliminaPendencia(userId)             
    achou = verificaUsuario(userId, bot)       
    if achou:    
        conn = conecta()
        if not conn:
            bot.send_message(userId, text="Erro ao tentar conectar ao Banco de Dados - opcaoInformaHorasAtividade")
            mostraMenuPrincipal(update, context)
            return
        if exibeAtividadesEmAndamento(bot, userId, conn)==0:
            conn.close()
            mostraMenuPrincipal(update, context) 
            return
        pendencias[userId] = 'informaHorasAtividade' #usuário agora tem uma pendência de informação (horas atividade)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, *o código da atividade e a quantidade de horas dispendidas* até o momento (número inteiro) - separe as informações com espaço:"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
        conn.close()
    else:
        mostraMenuPrincipal(update, context)   
    return    

def opcaoEnviaCienciasAtividades(update, context): #Envia para o e-mail do usuário relatório de ciências e atividades de TDPFs em andamento nos quais esteja alocado
    global pendencias
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id  
    bot = update.effective_user.bot 
    eliminaPendencia(userId)              
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        mostraMenuPrincipal(update, context)
        return                     
    conn = conecta()
    if not conn:
        bot.send_message(userId, text="Erro ao criar conexão ao Banco de Dados - opcaoEnviaCienciasAtividades")
        mostraMenuPrincipal(update, context)
        return
    consulta = """
                Select TDPFS.Numero, TDPFS.Emissao, TDPFS.Nome, Usuarios.email, TDPFS.Codigo
                From TDPFS, Usuarios, Alocacoes, Fiscais
                Where Usuarios.idTelegram=%s and Usuarios.Saida Is Null and Usuarios.Adesao Is Not Null and Usuarios.CPF=Fiscais.CPF and Fiscais.Codigo=Alocacoes.Fiscal
                and Alocacoes.Desalocacao Is Null and TDPFS.Codigo=Alocacoes.TDPF and TDPFS.Encerramento Is Null 
                Order by TDPFS.Numero
                """
    cursor = conn.cursor(buffered=True)           
    cursor.execute(consulta, (userId,))
    linhas = cursor.fetchall()
    bAchou = True
    if linhas==None:
        bAchou = False
    if len(linhas)==0:
        bAchou = False  
    if not bAchou:
        bot.send_message(userId, text="Não há TDPFs em andamento em que o usuário esteja alocado")
        mostraMenuPrincipal(update, context)
        conn.close()
        return  
    email = linhas[0][3]   
    if email==None or email=="": #email vazio
        bot.send_message(userId, text="Email do usuário não foi informado - não haverá envio.")
        mostraMenuPrincipal(update, context)  
        conn.close() 
        return 
    if not "@rfb.gov.br" in email: 
        bot.send_message(userId, text="Email do usuário é inválido - aparenta não ser institucional - não haverá envio.")
        mostraMenuPrincipal(update, context)  
        conn.close() 
        return                                            
    book = Workbook()
    sheet1 = book.active  
    sheet1.title = "Atividades"
    sheet1.cell(row=1, column=1).value = "TDPF"
    sheet1.cell(row=1, column=2).value = "Data Emissão"
    sheet1.cell(row=1, column=3).value = "Nome Fiscalizado"
    sheet1.cell(row=1, column=4).value = "Atividade"
    sheet1.cell(row=1, column=5).value = "Data de Início"            
    sheet1.cell(row=1, column=6).value = "Vencimento"
    sheet1.cell(row=1, column=7).value = "Término"
    sheet1.cell(row=1, column=8).value = "Horas"
    larguras = [19, 13, 32, 27, 13, 13, 13, 8]
    for col in range(len(larguras)):
        sheet1.column_dimensions[get_column_letter(col+1)].width = larguras[col]   
        currentCell = sheet1.cell(row=1, column=col+1)
        currentCell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)                 
    sheet2 = book.create_sheet(title="Ciências")
    sheet2.cell(row=1, column=1).value = "TDPF"
    sheet2.cell(row=1, column=2).value = "Data Emissão"
    sheet2.cell(row=1, column=3).value = "Nome Fiscalizado"
    sheet2.cell(row=1, column=4).value = "Data de Ciência"
    sheet2.cell(row=1, column=5).value = "Documento"            
    sheet2.cell(row=1, column=6).value = "60 dias da Ciência"  
    larguras = [19, 13, 32, 14, 25, 16]
    for col in range(len(larguras)):
        sheet2.column_dimensions[get_column_letter(col+1)].width = larguras[col]  
        currentCell = sheet2.cell(row=1, column=col+1)
        currentCell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)             
    i = 1
    j = 1
    for linha in linhas:                             
        tdpf = linha[0]
        chaveTdpf = linha[4]
        emissao = linha[1]
        if emissao==None:
            emissao = ""
        else:
            emissao = emissao.date()    
        fiscalizado = linha[2]
        if fiscalizado==None:
            fiscalizado = ""                
        consulta = "Select Atividade, Inicio, Vencimento, Termino, Horas from Atividades Where TDPF=%s Order By Inicio"
        cursor.execute(consulta,(chaveTdpf,))
        rows = cursor.fetchall()                  
        for row in rows:
            currentCell = sheet1.cell(row=i+1, column=1)
            currentCell.value = formataTDPF(tdpf)
            currentCell.alignment = Alignment(horizontal='center')             
            currentCell = sheet1.cell(row=i+1, column=2)
            currentCell.value = emissao
            currentCell.alignment = Alignment(horizontal='center')             
            sheet1.cell(row=i+1, column=3).value = fiscalizado                           
            sheet1.cell(row=i+1, column=4).value = row[0]
            if row[1]!=None:
                sheet1.cell(row=i+1, column=5).value = row[1].date()
                currentCell = sheet1.cell(row=i+1, column=5)
                currentCell.alignment = Alignment(horizontal='center')                 
            if row[2]!=None:                              
                sheet1.cell(row=i+1, column=6).value = row[2].date()
                currentCell = sheet1.cell(row=i+1, column=6)
                currentCell.alignment = Alignment(horizontal='center')                  
            if row[3]!=None:                              
                sheet1.cell(row=i+1, column=7).value = row[3].date()
            if row[4]!=None:
                sheet1.cell(row=i+1, column=8).value = row[4]
            if row[2]!=None and row[3]==None:
                cor = None
                if row[2].date()==datetime.now().date(): #se está vencendo hoje, fica azul
                    cor = Font(color="0000FF")
                if row[2].date()<datetime.now().date(): #se atividade está vencida, fica vermelha
                    cor = Font(color="FF0000") 
                if cor!=None:
                    for col in range(8):
                        sheet1.cell(row=i+1, column=col+1).font = cor
            i+=1  
        consulta = "Select Data, Documento from Ciencias Where TDPF=%s and Data Is Not Null Order By Data"
        cursor.execute(consulta,(chaveTdpf,))
        rows = cursor.fetchall()      
        totalRows = len(rows) 
        rowAtual = 1           
        for row in rows: 
            currentCell = sheet2.cell(row=j+1, column=1)
            currentCell.value = formataTDPF(tdpf)
            currentCell.alignment = Alignment(horizontal='center')             
            currentCell = sheet2.cell(row=j+1, column=2)
            currentCell.value = emissao
            currentCell.alignment = Alignment(horizontal='center')             
            sheet2.cell(row=j+1, column=3).value = fiscalizado                           
            currentCell = sheet2.cell(row=j+1, column=4)
            currentCell.value = row[0].date()
            currentCell.alignment = Alignment(horizontal='center')             
            if row[1]!=None:
                sheet2.cell(row=j+1, column=5).value = row[1] 
            diaEspont = (row[0]+timedelta(days=60)).date()
            sheet2.cell(row=j+1, column=6).value = diaEspont
            cor = None
            if diaEspont<=(datetime.now()+timedelta(days=15)).date() and rowAtual==totalRows: #se faltar 15 dias ou menos para recuperar a espontaneidade, 
                                                                                              #a linha fica azul, mas só se for na última ciência
                cor = Font(color="0000FF")
            if diaEspont<datetime.now().date() and rowAtual==totalRows: #se a espontaneidade já foi recuperada, a linha fica vermelha na última ciência
                cor = Font(color="FF0000") 
            elif diaEspont<datetime.now().date(): #se a espontaneidade tiver sido recuperada em ciência que não é a última, fica na cor roxa
                #temos que verificar em relação à ciência seguinte (há uma próxima ciência)
                if (rows[rowAtual][0]-row[0]).days>60: #entre a ciência atual e a subsequente, decorreram mais de 60 dias
                    cor = Font(color="800080")
            if cor!=None:
                for col in range(6):
                    sheet2.cell(row=j+1, column=col+1).font = cor   
            rowAtual+=1         
            j+=1
    if i>1 or j>1:              
        nomeArq = "CiencAtiv_"+str(userId)+"_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".xlsx"
        book.save(nomeArq)   
        message = "Prezado(a),\n\nConforme solicitado, enviamos, em anexo, planilha com relação das ciências e das atividades de TDPFs em andamento sob sua responsabilidade.\n\nAtenciosamente,\n\nDisav/Cofis\n\nAmbiente: "+ambiente    
        resultado = enviaEmail(email, message, "Relação de Ciências e Atividades - TDPFs", nomeArq)
        if resultado!=3:
            msg = "Erro no envio de email - opcaoEnviaCienciasAtividades - "+str(resultado)
            logging.info(msg + " - "+email)
            bot.send_message(userId, text=msg)
        else:
            bot.send_message(userId, text="E-mail enviado.")   
        os.remove(nomeArq)
    else:    
        bot.send_message(userId, text="Não há atividades e ciências informadas relativamente aos TDPFs em andamento sob sua responsabilidade.")
    conn.close()
    mostraMenuPrincipal(update, context)         
    return   

def opcaoInformaCiencia(update, context): #Informa ciência de TDPF
    global pendencias
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id  
    bot = update.effective_user.bot 
    eliminaPendencia(userId)              
    achou = verificaUsuario(userId, bot)       
    if achou:                      
        pendencias[userId] = 'ciencia' #usuário agora tem uma pendência de informação (ciência)
        response_message = "Envie /menu para ver o menu principal. Envie agora, numa única mensagem, *o nº do TDPF (16 dígitos) e a data de ciência (dd/mm/aaaa)* válida para fins de perda da espontaneidade tributária relativa ao respectivo procedimento fiscal - separe as informações com espaço:"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    else:
        mostraMenuPrincipal(update, context) 
    return
        
def opcaoPrazos(update, context): #Informa prazos para receber avisos
    global pendencias, textoRetorno  
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id 
    bot = update.effective_user.bot     
    eliminaPendencia(userId)     
    comando = "Select d1, d2, d3, Saida from Usuarios Where idTelegram=%s"
    saida = None
    conn = conecta()
    if not conn:
        response_message = "Erro na conexão (5)"
        bot.send_message(userId, text=response_message)
        return     
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
            response_message = "Envie /menu para ver o menu principal. Prazos vigentes para receber alertas: *{} (d1), {} (d2) e {} (d3) dias* antes de o contribuinte readquirir a espontaneidade.\nEnvie agora, numa única mensagem, *três quantidades de dias (1 a 50) distintas* antes de o contribuinte readquirir a espontaneidade tributária em que você deseja receber alertas (separe as informações com espaço):".format(d1, d2, d3)
    bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    conn.close()  
    return

def opcaoAnulaCiencia(update, context): #Anula ciência de TDPF
    global pendencias    
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id  
    bot = update.effective_user.bot    
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:            
        pendencias[userId] = 'anulaCiencia'  #usuário agora tem uma pendência de informação   
        response_message = "Envie /menu para ver o menu principal. Envie agora *o nº do TDPF (16 dígitos)* para o qual você deseja anular a última ciência informada que impedia a recuperação da espontaneidade (retornará para a anterior):"
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    else:
        mostraMenuPrincipal(update, context)         
    return

def opcaoFinalizaAvisos(update, context): #Finaliza avisos para um certo TDPF
    global pendencias    
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot      
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'fim'     #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora *o nº do TDPF (16 dígitos) ou a palavra TODOS* para finalizar alertas/monitoramento de um ou de todos os TDPFs:"
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
    else:
        mostraMenuPrincipal(update, context)         
    return

def opcaoAcompanhaTDPFs(update, context): #acompanha um TDPF ou todos os TDPFs em que estiver alocado ou em que for supervisor
    global pendencias    
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'acompanha' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora o *nº do TDPF (16 dígitos) ou a palavra TODOS* para receber alertas relativos ao TDPF informado ou a todos em que estiver alocado e/ou que for supervisor:"  
        bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')
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
            comando = '''Select TDPFS.Numero, TDPFS.Vencimento, TDPFS.Codigo, Alocacoes.Supervisor from CadastroTDPFs, Alocacoes, Fiscais, TDPFS
                        Where Fiscais.CPF=%s and CadastroTDPFs.Fiscal=Fiscais.Codigo and CadastroTDPFs.Fim Is Null and CadastroTDPFs.Fiscal=Alocacoes.Fiscal and 
                        CadastroTDPFs.TDPF=Alocacoes.TDPF and CadastroTDPFs.TDPF=TDPFS.Codigo and Alocacoes.Desalocacao Is Null and 
                        TDPFS.Encerramento Is Null Order By TDPFS.Numero'''
        elif tipo==2:
        #seleciona todos os TDPFs dos quais o usuário é supervisor
            comando = """ 
                        Select TDPFS.Numero, TDPFS.Vencimento, TDPFS.Codigo from TDPFS, Supervisores, Fiscais Where Encerramento Is Null and 
                        Supervisores.Equipe=TDPFS.Grupo and Supervisores.Fim Is Null and Fiscais.CPF=%s and Fiscais.Codigo=Supervisores.Fiscal Order by TDPFS.Numero
                        """
            #comando = '''Select Alocacoes.TDPF as tdpf, Vencimento from Alocacoes, TDPFS Where Desalocacao Is Null and Encerramento Is Null 
            #            and CPF=%s and TDPF=Numero and Supervisor='S' Order by TDPF'''
        else:
            conn.close()
            return None  
        cursor.execute(comando, (cpf,))
        listaAux = cursor.fetchall()
        if not listaAux:
            conn.close()
            return None
        if len(listaAux)==0:
            conn.close()
            return None            
        result = []
        for linha in listaAux:
            tdpf = linha[0]
            chaveTdpf = linha[2]
            vencimento = linha[1]
            if vencimento:
                vencimento = vencimento.date()
                vctoTDPFNum = (vencimento-datetime.today().date()).days
                vctoTDPF = str(vctoTDPFNum)
                if vctoTDPFNum<0:
                    vctoTDPF = vctoTDPF + " (vencido)"
            else:
                vctoTDPF = "ND"
            comando = "Select Data, Documento from Ciencias Where TDPF=%s order by Data DESC"
            #logging.info(comando)
            cursor.execute(comando, (chaveTdpf,))            
            cienciaReg = cursor.fetchone() #busca a data de ciência mais recente (DESC acima)
            if tipo==2: #verificamos se o TDPF está sendo monitorado
                comando = "Select Codigo as codigo from CadastroTDPFs Where TDPF=%s and Fim Is Null"
                cursor.execute(comando, (chaveTdpf,))
                monitoradoReg = cursor.fetchone()
                if monitoradoReg:
                    monitorado = "SIM"
                else:
                    monitorado = "NÃO"       
            tdpfForm = formataTDPF(tdpf)
            documento = ""
            if cienciaReg:
                if len(cienciaReg)>0: 
                    ciencia = cienciaReg[0] #obtem a data de ciência mais recente
                    documento = cienciaReg[1]
                else:
                    ciencia = None    
            else:
                ciencia = None
            if tipo==1:
                atividades = []
                comando = "Select Codigo, Atividade, Inicio, Vencimento, Horas from Atividades Where TDPF=%s and Termino Is Null order by Inicio"   #somente as atividade em andamento
                cursor.execute(comando, (chaveTdpf,))
                regAtividades = cursor.fetchall()
                for regAtividade in regAtividades:
                    lista = []
                    lista.append(regAtividade[0])
                    lista.append(regAtividade[1])
                    lista.append(regAtividade[2])
                    lista.append(regAtividade[3])
                    lista.append(regAtividade[4])
                    atividades.append(lista)            
                registro = [tdpfForm, linha[3], ciencia, documento, vctoTDPF, atividades]
            else:
                registro = [tdpfForm, monitorado, ciencia, documento, vctoTDPF]
            result.append(registro)       
        if len(result)>0:
            #logging.info(result)
            conn.close()
            return result
        else:
            conn.close()
            return None
    except:
        conn.close()
        return ["Erro na consulta (9). Tente novamente mais tarde."]
            
        
def opcaoMostraTDPFs(update, context): #Relação de TDPFs monitorados, prazos e atividades do fiscal alocado
    global pendencias, ambiente   
    if update.effective_user.is_bot:
        return #não atendemos bots        
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
            documento = item[3]
            vctoTDPF = item[4]
            if documento==None:
                documento = "ND"
            if vctoTDPF==None:
                vctoTDPF = "ND"    
            atividades.append([tdpf, item[5]]) #item[5] é uma lista de atividades
            if ciencia:
                delta = ciencia.date() + timedelta(days=60)-datetime.today().date()
                dias = delta.days
                if dias<0:
                    dias = "d) "+str(dias)+" (recuperada); e) "+documento+"; f) "+vctoTDPF
                else:
                    dias = "d) "+ str(dias) + "; e) "+documento+"; f) "+vctoTDPF    
                msg = msg+"\n\n"+str(i)+"a) *"+tdpf+"*; b) "+supervisor+";\nc) "+ciencia.strftime('%d/%m/%Y')+"; "+dias
            else:
                msg = msg+"\n\n"+str(i)+"a) *"+tdpf+"*; b) "+supervisor+"\nc) ND; d) ND; e) ND; f) "+vctoTDPF  
            if i%15==0:   #há limite de tamanho de msg - enviamos 15 TDPFs por vez, no máximo
                response_message = "TDPFs Monitorados Por Você:\na) TDPF; b) Supervisor; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Documento; f) Dias restantes para o vencto. do TDPF:"
                response_message = response_message+msg  
                if ambiente=="TESTE":
                    response_message = response_message+"\n\nAmbiente: "+ambiente   
                bot.send_message(userId, text=response_message)               
                msg = ""                       
            i+=1                 
        if msg!="":
            response_message = "TDPFs Monitorados Por Você:\na) TDPF; b) Supervisor; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Documento; f) Dias restantes para o vencto. do TDPF:"
            response_message = response_message+msg
            response_message = response_message + "\n\nVencimento do TDPF pode ser inferior ao do Ação Fiscal, pois as informações do serviço se baseiam"
            response_message = response_message + " na data de distribuição, que pode ter ocorrido antes da assinatura e emissão do TDPF."             
            if ambiente=="TESTE":
                response_message = response_message+"\n\nAmbiente: "+ambiente
            response_message = limpaMarkdown(response_message)
            bot.send_message(userId, text=response_message, parse_mode= 'MarkdownV2')                
        response_message = ""         
        msg = ""
        i = 1
        for atividade in atividades:
            logging.info(atividade)
            for registro in atividade[1]:
                horas = registro[4]
                if horas==None:
                    horas = 0
                msg = msg + "\n\n"+str(i)+"a) *"+atividade[0]+"*; b) "+str(registro[0])+"; c) "+registro[1]+"; d) "+registro[2].strftime('%d/%m/%Y')+"; e) "+registro[3].strftime('%d/%m/%Y')+"; f) "+str(horas)
                i+=1
        if msg!="":
            response_message = "Lista de atividades em andamento dos TDPFs Monitorados:\na)TDPF; b) Código; c) Descrição; d) Início; e) Vencimento; f) Horas" + msg            
            if ambiente=="TESTE":
                response_message = response_message+"\n\nAmbiente: "+ambiente			
    if response_message!="": 
        response_message = limpaMarkdown(response_message)
        bot.send_message(userId, text=response_message, parse_mode= 'MarkdownV2')                
    mostraMenuPrincipal(update, context)
    return

def opcaoMostraSupervisionados(update, context): #acompanha um TDPF ou todos os TDPFs em que estiver alocado ou em que for supervisor
    global pendencias    
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'mostraSuperv' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Deseja que envie também e-mail (SIM ou NÃO)?"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context)         
    return

def mostraSupervisionados(update, context): #Relação de TDPFs supervisionados pelo usuário
    global pendencias, textoRetorno, ambiente
    userId = update.message.from_user.id   
    bot = update.effective_user.bot
    if update.effective_user.is_bot:
        return #não atendemos bots       
    msg = update.message.text    
    parametros = getParametros(msg)
    if len(parametros)!=1:
        response_message = "Envie somente SIM, S, NÃO ou N."
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return  
    resposta = parametros[0].upper()
    if not resposta in ["SIM", "S", "NÃO", "N", "NAO"]:
        response_message = "Envie somente SIM, S, NÃO ou N."
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)
        return          
    resposta = resposta[:1]                 
    eliminaPendencia(userId)       
    lista = montaListaTDPFs(userId, 2)
    if lista==None:
        response_message = "Você não supervisiona nenhum TDPF." 
        bot.send_message(userId, text=response_message)
        mostraMenuPrincipal(update, context)  
        return             
    if len(lista)==1 and type(lista[0]) is str:
        response_message = lista[0]
        bot.send_message(userId, text=response_message) 
        mostraMenuPrincipal(update, context)         
        return 
    if resposta=='S':
        book = Workbook()
        sheet = book.active  
        sheet.cell(row=1, column=1).value = "TDPF"
        sheet.cell(row=1, column=2).value = "Data Emissão"
        sheet.cell(row=1, column=3).value = "Nome Fiscalizado"
        sheet.cell(row=1, column=4).value = "Auditor-Fiscal"
        sheet.cell(row=1, column=5).value = "Monitorado"
        sheet.cell(row=1, column=6).value = "Última Ciência"
        sheet.cell(row=1, column=7).value = "Dias p/ Recuperação da Espontaneidade"   
        sheet.cell(row=1, column=8).value = "Documento que Efetivou a Ciência"            
        sheet.cell(row=1, column=9).value = "Dias p/ Vencimento do TDPF"
        sheet.cell(row=1, column=10).value = "Horas Alocadas (TODOS os AFRFBs)"
        sheet.row_dimensions[1].height = 42    
        larguras = [18, 13, 32, 32, 11, 13, 14.5, 25, 13, 13.5]
        for col in range(len(larguras)):
            sheet.column_dimensions[get_column_letter(col+1)].width = larguras[col]  
            currentCell = sheet.cell(row=1, column=col+1)
            currentCell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)                     
    i = 1
    msg = ""
    conn = conecta()  
    if conn:
        cursor = conn.cursor(buffered=True)       
    for item in lista:
        tdpf = item[0]
        monitorado = item[1]
        ciencia = item[2]
        documento = item[3]
        vctoTDPF = item[4]
        if vctoTDPF==None:
            vctoTDPF = "ND"
        if documento==None:
            documento = ""
        email = ""
        consulta = """
                    Select TDPFS.Emissao, TDPFS.Nome, Fiscais.Nome, TDPFS.Codigo From TDPFS, Alocacoes, Fiscais 
                    Where TDPFS.Numero=%s and TDPFS.Codigo=Alocacoes.TDPF and Alocacoes.Desalocacao Is Null and
                    Alocacoes.Supervisor='N' and Alocacoes.Fiscal=Fiscais.Codigo
                    """
        fiscal = ""
        fiscalizado = ""
        emissao = None   
        chaveTdpf = None                    
        if conn:
            cursor.execute(consulta, (getAlgarismos(tdpf),))
            linhas = cursor.fetchall()
            for linha in linhas:
                emissao = linha[0]
                if emissao!=None:
                    emissao = emissao.date()
                else:
                    emissao = ""    
                fiscalizado = linha[1]
                fiscal = linha[2]
                chaveTdpf = linha[3]
                break
        if resposta=='S':
            consulta = "Select email from Usuarios Where idTelegram=%s"  
            cursor.execute(consulta, (userId,))   
            linhas = cursor.fetchall()
            for linha in linhas:
                email = linha[0]
                break          
            currentCell = sheet.cell(row=i+1, column=1)
            currentCell.value = tdpf
            currentCell.alignment = Alignment(horizontal='center') 
            currentCell = sheet.cell(row=i+1, column=2)
            currentCell.value = emissao
            currentCell.alignment = Alignment(horizontal='center')             
            sheet.cell(row=i+1, column=3).value = fiscalizado
            sheet.cell(row=i+1, column=4).value = fiscal
            currentCell = sheet.cell(row=i+1, column=5)
            currentCell.value = monitorado
            currentCell.alignment = Alignment(horizontal='center')              
            sheet.cell(row=i+1, column=9).value = getAlgarismos(vctoTDPF)
            consulta = "Select SUM(Horas) from Alocacoes Where TDPF=%s"
            if conn:
                cursor.execute(consulta,(chaveTdpf,))
                horasAloc = cursor.fetchone()
                if horasAloc:
                    horas = horasAloc[0]
                else:
                    horas = 0    
                sheet.cell(row=i+1, column=10).value = horas
        if ciencia:
            delta = ciencia.date() + timedelta(days=60)-datetime.today().date()
            dias = delta.days
            if resposta=='S':
                currentCell = sheet.cell(row=i+1, column=6)
                currentCell.value = ciencia.date()
                currentCell.alignment = Alignment(horizontal='center')                 
                sheet.cell(row=i+1, column=7).value = int(dias)
                sheet.cell(row=i+1, column=8).value = documento
                cor = None
                if dias<=15: #se faltar 15 dias ou menos para recuperar a espontaneidade, a linha fica azul
                    cor = Font(color="0000FF")
                if dias<0: #se a espontaneidade já foi recuperada, a linha fica vermelha
                    cor = Font(color="FF0000") 
                if cor!=None:
                    for col in range(5,8):
                        sheet.cell(row=i+1, column=col+1).font = cor                 
            if dias<0:
                dias = " d) "+str(dias)+" (recuperada); e) "+vctoTDPF
            else:
                dias = " d) "+ str(dias) + "; e) "+vctoTDPF
            msg = msg+"\n\n"+str(i)+"a) *"+tdpf+"*("+fiscal.strip().split()[0]+"); b) "+monitorado+"; c) "+ciencia.strftime('%d/%m/%Y')+";"+dias
        else:
            msg = msg+"\n\n"+str(i)+"a) "+tdpf+"; b) "+monitorado+";c) ND; d) ND; e) "+vctoTDPF              
        if (i % 15) == 0: #há limite de tamanho de msg - enviamos 15 TDPFs por vez, no máximo
            response_message = "TDPFs Supervisionados Por Você:\na) TDPF; b) Monitorado Por Algum Fiscal; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Dias restantes para o vencto. do TDPF:" 
            response_message = response_message + msg  
            response_message = response_message + "\n\nVencimento do TDPF pode ser inferior ao do Ação Fiscal, pois as informações do serviço se baseiam"
            response_message = response_message + " na data de distribuição, que pode ter ocorrido antes da assinatura e emissão do TDPF."            
            if ambiente=="TESTE":
                response_message = response_message+"\n\nAmbiente: "+ambiente
            response_message = response_message.replace(".", "\.").replace("_", "\_").replace("[", "\[").replace("]", "\]").replace(")", "\)").replace("(", "\(").replace("-","\-")#.replace("*", "\\*")        
            bot.send_message(userId, text=response_message, parse_mode= 'MarkdownV2')                  	                                  
            msg = ""
        i+=1  
    if conn:
        conn.close()                   
    if msg!="":    
        response_message = "TDPFs Supervisionados Por Você:\na) TDPF; b) Monitorado Por Algum Fiscal; c) Data da última ciência; d) Dias restantes p/ recuperação da espontaneidade; e) Dias restantes para o vencto. do TDPF:"  
        response_message = response_message+msg
        if ambiente=="TESTE":
            response_message = response_message+"\n\nAmbiente: "+ambiente		
            response_message = limpaMarkdown(response_message)
            bot.send_message(userId, text=response_message, parse_mode= 'MarkdownV2')   
    if resposta=='S':
        if email!="" and "@rfb.gov.br" in email:
            nomeArq = "Sup_"+str(userId)+"_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".xlsx"
            book.save(nomeArq)   
            message = "Prezado(a),\n\nConforme solicitado, enviamos, em anexo, planilha com relação dos TDPFs sob sua supervisão.\n\nAtenciosamente,\n\nDisav/Cofis\n\nAmbiente: "+ambiente    
            resultado = enviaEmail(email, message, "Relação de TDPFs Supervisionados", nomeArq)
            if resultado!=3:
                msg = "Erro no envio de email - mostraSupervisionados - "+str(resultado)
                logging.info(msg + " - "+email)
                bot.send_message(userId, text=msg)
            else:
                bot.send_message(userId, text="E-mail enviado.")   
            os.remove(nomeArq)  
        conn.close()    
    mostraMenuPrincipal(update, context)
    return

def opcaoEnviaAtividades(update, context): #envia relação de atividades de TDPFs em andamento para o supervisor
    global textoRetorno
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot  
    eliminaPendencia(userId)          
    achou = verificaUsuario(userId, bot)       
    if achou:   
        conn = conecta()
        if not conn:
            bot.send_message(userId, text="Erro ao criar conexão ao Banco de Dados - opcaoEnviaAtividades")
            mostraMenuPrincipal(update, context)
            return
        consulta = """
                   Select TDPFS.Numero, TDPFS.Emissao, TDPFS.Nome, Usuarios.email, TDPFS.Codigo
                   From TDPFS, Usuarios, Supervisores, Fiscais
                   Where Usuarios.idTelegram=%s and Usuarios.Saida Is Null and Usuarios.Adesao Is Not Null and TDPFS.Encerramento Is Null 
                   and Usuarios.CPF=Fiscais.CPF and Fiscais.Codigo=Supervisores.Fiscal and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo
                   Order by TDPFS.Numero
                   """
        cursor = conn.cursor(buffered=True)           
        cursor.execute(consulta, (userId,))
        linhas = cursor.fetchall()
        bAchou = True
        if linhas==None:
            bAchou = False
        if len(linhas)==0:
            bAchou = False  
        if not bAchou:
            bot.send_message(userId, text="Não há TDPFs sob sua supervisão ou usuário está inativo no serviço - opcaoEnviaAtividades")
            mostraMenuPrincipal(update, context)
            conn.close()
            return  
        email = linhas[0][3]   
        if email==None or email=="": #email vazio
            bot.send_message(userId, text="Email do usuário não foi informado - não haverá envio.")
            mostraMenuPrincipal(update, context)  
            conn.close() 
            return 
        if not "@rfb.gov.br" in email: 
            bot.send_message(userId, text="Email do usuário é inválido - aparenta não ser institucional - não haverá envio.")
            mostraMenuPrincipal(update, context)  
            conn.close() 
            return                                            
        book = Workbook()
        sheet = book.active  
        sheet.cell(row=1, column=1).value = "TDPF"
        sheet.cell(row=1, column=2).value = "Data Emissão"
        sheet.cell(row=1, column=3).value = "Nome Fiscalizado"
        sheet.cell(row=1, column=4).value = "Auditor-Fiscal"
        sheet.cell(row=1, column=5).value = "Atividade"
        sheet.cell(row=1, column=6).value = "Data de Início"            
        sheet.cell(row=1, column=7).value = "Vencimento"
        sheet.cell(row=1, column=8).value = "Término"
        sheet.cell(row=1, column=9).value = "Horas"
        larguras = [19, 13, 32, 32, 27, 14, 13, 13, 10]
        for col in range(len(larguras)):
            sheet.column_dimensions[get_column_letter(col+1)].width = larguras[col]  
            currentCell = sheet.cell(row=1, column=col+1)
            currentCell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)         
        i = 1
        for linha in linhas:  
            chaveTdpf = linha[4]                           
            tdpf = linha[0]
            emissao = linha[1]
            if emissao==None:
                emissao = ""
            else:
                emissao = emissao.date()    
            fiscalizado = linha[2]
            if fiscalizado==None:
                fiscalizado = ""                
            consulta = "Select Atividade, Inicio, Vencimento, Termino, Horas from Atividades Where TDPF=%s Order by Inicio"
            cursor.execute(consulta,(chaveTdpf,))
            rows = cursor.fetchall()
            if not rows:
                continue
            if len(rows)==0:
                continue 
            fiscal = ""
            consulta = "Select Fiscais.Nome From Alocacoes, Fiscais Where Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=%s"
            cursor.execute(consulta,(chaveTdpf,))
            rowFisc = cursor.fetchone()
            if rowFisc:
                if len(rowFisc)>0:
                    fiscal = rowFisc[0]  
                    #print(fiscal)                       
            for row in rows:
                currentCell = sheet.cell(row=i+1, column=1)
                currentCell.value = formataTDPF(tdpf)
                currentCell.alignment = Alignment(horizontal='center')                                             
                currentCell = sheet.cell(row=i+1, column=2)
                currentCell.value = emissao
                currentCell.alignment = Alignment(horizontal='center')                
                sheet.cell(row=i+1, column=3).value = fiscalizado 
                sheet.cell(row=i+1, column=4).value = fiscal                             
                sheet.cell(row=i+1, column=5).value = row[0]
                if row[1]!=None:
                    sheet.cell(row=i+1, column=6).value = row[1].date()
                if row[2]!=None:                              
                    sheet.cell(row=i+1, column=7).value = row[2].date()
                if row[3]!=None:                              
                    sheet.cell(row=i+1, column=8).value = row[3].date() 
                if row[4]!=None:                              
                    sheet.cell(row=i+1, column=9).value = row[4]
                for col in range(6, 9):
                    currentCell = sheet.cell(row=i+1, column=col)
                    currentCell.alignment = Alignment(horizontal='center')                    
                if row[2]!=None and row[3]==None:
                    cor = None
                    if row[2].date()==datetime.now().date(): #se a atividade está vencendo, a linha fica azul
                        cor = Font(color="0000FF")
                    if row[2].date()<datetime.now().date(): #se a atividade está vencida, a linha fica vermelha
                        cor = Font(color="FF0000") 
                    if cor!=None:
                        for col in range(9):
                            sheet.cell(row=i+1, column=col+1).font = cor                                     
                i+=1          
        if i>1:
            nomeArq = "SupAtiv_"+str(userId)+"_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".xlsx"
            book.save(nomeArq)   
            message = "Prezado(a),\n\nConforme solicitado, enviamos, em anexo, planilha com relação das atividades de TDPFs em andamento sob sua supervisão.\n\nAtenciosamente,\n\nDisav/Cofis\n\nAmbiente: "+ambiente    
            resultado = enviaEmail(email, message, "Relação de Atividades - TDPFs Supervisionados", nomeArq)
            if resultado!=3:
                msg = "Erro no envio de email - opcaoEnviaAtividades - "+str(resultado)
                logging.info(msg + " - "+email)
                bot.send_message(userId, text=msg)
            else:
                bot.send_message(userId, text="E-mail enviado.")   
            os.remove(nomeArq)
        else:    
            bot.send_message(userId, text="Não há atividades relativamente aos TDPFs em andamento sob sua supervisão.")
        conn.close()
    mostraMenuPrincipal(update, context)         
    return   

def opcaoSupervisorEspontaneidade(update, context): #exibição de TDPFS que estejam vencendo hoje ou em X dias para o supervisor
    global pendencias    
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot     
    eliminaPendencia(userId)  
    achou = verificaUsuario(userId, bot)       
    if achou:   
        pendencias[userId] = 'supervisorEspontaneidade' #usuário agora tem uma pendência de informação
        response_message = "Envie /menu para ver o menu principal. Envie agora um intervalo de dias (um ou dois dígitos para cada) em que haverá a recuperação da espontaneidade tributária - TDFPs de sua EQUIPE:"  
        bot.send_message(userId, text=response_message)
    else:
        mostraMenuPrincipal(update, context)         
    return

def mostraSupervisorEspontaneidade(update, context): #exibe os TDPFs da equipe que estão recuperando a espontaneidade em até X dias
    global pendencias, textoRetorno
    if update.effective_user.is_bot:
        return #não atendemos bots       
    userId = update.effective_user.id
    bot = update.effective_user.bot       
    eliminaPendencia(userId) 
    achou = verificaUsuario(userId, bot)       
    if not achou: 
        mostraMenuPrincipal(update, context)         
        return
    msg = update.message.text  
    prazo = [0, 0]    
    parametros = getParametros(msg)  
    if len(parametros)==1 and parametros[0].isdigit:
        try:
            prazo[0] = int(parametros[0])
            prazo[1] = prazo[0]
        except:    
            response_message = "Envie somente dois números inteiros (separados por espaço)."
            response_message = response_message+textoRetorno  
            bot.send_message(userId, text=response_message)  
            return
    elif len(parametros)==3 and parametros[0].isdigit and parametros[1].upper()=="A" and parametros[2].isdigit():
        try:
            prazo[0] = int(parametros[0])
            prazo[1] = int(parametros[2])
        except:    
            response_message = "Envie somente dois números inteiros (separados por espaço) (2)."
            response_message = response_message+textoRetorno  
            bot.send_message(userId, text=response_message)  
            return
    elif len(parametros)!=2:
        response_message = "Envie somente dois números inteiros (separados por espaço) (3)."
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return
    else: 
        resposta1 = parametros[0]
        resposta2 = parametros[1]
        if resposta1.isdigit() and resposta2.isdigit():
            try:
                prazo[0] = int(resposta1)
                prazo[1] = int(resposta2)
            except:
                response_message = "Envie somente dois números inteiros (separados por espaço) (4)."
                response_message = response_message+textoRetorno  
                bot.send_message(userId, text=response_message)  
                return 
        else:
            response_message = "Envie somente dois números inteiros (separados por espaço) (5)."
            response_message = response_message+textoRetorno  
            bot.send_message(userId, text=response_message)  
            return  
    prazo.sort()  
    if prazo[0]>99 or prazo[1]>99 or prazo[0]<0 or prazo[1]<0:
        response_message = "Envie somente dois números inteiros positivos (separados por espaço) de no máximo dois dígitos cada."
        response_message = response_message+textoRetorno  
        bot.send_message(userId, text=response_message)  
        return                           
    conn = conecta()
    if not conn:
        bot.send_message(userId, text="Erro na conexão - mostraSupervisorEspontaneidade")
        return        
    cursor = conn.cursor(buffered=True)
    consulta = """
                Select TDPFS.Numero, TDPFS.Codigo
                From TDPFS, Usuarios, Supervisores, Fiscais
                Where Usuarios.idTelegram=%s and Usuarios.Saida Is Null and TDPFS.Encerramento Is Null and Usuarios.CPF=Fiscais.CPF and Fiscais.Codigo=Supervisores.Fiscal and 
                Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Null
                """
    cursor.execute(consulta, (userId,))
    linhas = cursor.fetchall()
    msg = ""
    i = 1
    for linha in linhas:
        chaveTdpf = linha[1]
        tdpf = linha[0]
        consulta = "Select Fiscais.Nome from Alocacoes, Fiscais Where Alocacoes.TDPF=%s and Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.Desalocacao Is Null"
        cursor.execute(consulta, (chaveTdpf,))
        linhaFiscal = cursor.fetchone()
        if linhaFiscal:
            fiscalPrimNome = linhaFiscal[0].strip().split()[0]
        else:
            fiscalPrimNome = "ND"
        consulta = "Select Ciencias.Data, Ciencias.Documento From Ciencias Where Ciencias.TDPF=%s Order By Ciencias.Data DESC"
        cursor.execute(consulta, (chaveTdpf,))
        ciencia = cursor.fetchone() #busca a última
        if ciencia:
            if ciencia[0]!=None:
                dataCiencia = ciencia[0].date()
                prazoRestante = (dataCiencia+timedelta(days=60)-datetime.now().date()).days 
                if prazoRestante>=prazo[0] and prazoRestante<=prazo[1]:
                    if ciencia[1]==None:
                        documento = "ND"
                    else:
                        documento = ciencia[1]
                    if msg!="":
                        msg = msg + "\n"
                    msg = msg +"\n"+str(i)+") *TDPF: "+formataTDPF(tdpf)+"* ("+fiscalPrimNome+"); Documento: "+documento+"; Ciência: "+dataCiencia.strftime("%d/%m/%Y")+"; Recupera em "+str(prazoRestante)+" dias"
                    i+=1
    if msg!="":
        response_message = "Relação de TDPFs cuja recuperação da espontaneidade tributária ocorrerá em "+str(prazo[0])+" a "+str(prazo[1])+" dias:"+msg
        response_message = limpaMarkdown(response_message)
    else:
        response_message = "Não haverá recuperação da espontaneidade tributária para nenhum TDPF neste intervalo."        
    bot.send_message(userId, text=response_message, parse_mode= 'MarkdownV2')                          
    conn.close()
    mostraMenuPrincipal(update, context)  
    return
    
def opcaoEMail(update, context): #cadastra e-mail para o recebimento de avisos
    global pendencias
    if update.effective_user.is_bot:
        return #não atendemos bots       
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
        bot.send_message(userId, text="Erro na consulta (10).")  
        conn.close()      
        return False     
    pendencias[userId] = 'email'     #usuário agora tem uma pendência de informação
    if email!=None and email!="":
        response_message = "Envie /menu para ver o menu principal. Email atualmente cadastrado - "+email+". Informe seu *novo nome de usuário do endereço de e-mail institucional ou a palavra NULO* para descadastrar o atual (exemplo - se seu e-mail é fulano@rfb.gov.br, envie fulano):"
    else:    
        response_message = "Envie /menu para ver o menu principal. Envie agora seu *nome de usuário do endereço de e-mail institucional* no qual você também receberá alertas (exemplo - se seu e-mail é fulano@rfb.gov.br, envie fulano):"
    bot.send_message(userId, text=limpaMarkdown(response_message), parse_mode= 'MarkdownV2')  
    return  

############################# Handlers #########################################
def botTelegram():
    global updater, token, senhaAvisoUrgente
    logging.info('Tentando iniciar o Updater Telegram')
    updater = Updater(token, use_context=True) 
    
    updater.dispatcher.add_handler(CommandHandler('start', start))
    #updater.dispatcher.add_handler(CallbackQueryHandler(menuTDPF))    
    updater.dispatcher.add_handler(CommandHandler('menu', start))
    updater.dispatcher.add_handler(CommandHandler('retorna', start)) 
    if len(senhaAvisoUrgente)>=6:
        updater.dispatcher.add_handler(MessageHandler(Filters.regex('DisparaAvisoUrgente'+senhaAvisoUrgente), disparaAvisoUrgente))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Menu Principal'), mostraMenuPrincipal))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Cadastros'), mostraMenuCadastro))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('TDPF - Monitoramento'), mostraMenuTDPF))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Menu TDPF - Monitoramento'), mostraMenuTDPF))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Espontaneidade e Atividades Relativas a TDPF'), mostraMenuCienciasAtividades))    
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Prazos Para Receber Avisos'), opcaoPrazos))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Registra Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Solicita Chave de Registro e do ContÁgil'), solicitaChaveRegistro))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Desativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Reativa Usuário'), opcaoUsuario))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Cadastra/Exclui e-Mail'), opcaoEMail))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Data de Ciência'), opcaoInformaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Anula Data de Ciência Informada'), opcaoAnulaCiencia))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Ativ. e Prazo'), opcaoInformaAtividade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Anula Atividade'), opcaoAnulaAtividade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Finaliza Monitoramento de TDPF'), opcaoFinalizaAvisos))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Mostra TDPFs Monitorados'), opcaoMostraTDPFs))     
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Monitora TDPF\(s\)'), opcaoAcompanhaTDPFs))   
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Supervisão'), mostraMenuSupervisao))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Mostra TDPFs Supervisionados'), opcaoMostraSupervisionados)) 
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Envia Atividades \(e-Mail\) - Superv.'), opcaoEnviaAtividades))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Recuperação Espontaneidade - Superv.'), opcaoSupervisorEspontaneidade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Término de Ativ.'), opcaoInformaTerminoAtividade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Informa Horas Atividade'), opcaoInformaHorasAtividade))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Ciências e Atividades - Email'), opcaoEnviaCienciasAtividades))
    updater.dispatcher.add_handler(MessageHandler(Filters.regex('Inicio'), mostraMenuPrincipal))          
    updater.dispatcher.add_handler(MessageHandler(Filters.all, unknown)) 
    
    updater.start_polling()
    logging.info('Serviço iniciado - '+datetime.now().strftime('%d/%m/%Y %H:%M'))
    #updater.idle()    #não é necessário pq o programa vai ficar rodando em loop infinito
################################################################################

def disparaAvisoUrgente(update, context): #avisos urgentes da Cofis disparados por comando no Bot (exige comando especifico e senha)
    global updater, ambiente
    if update.effective_user.is_bot:
        return #não atendemos bots    
    conn = conecta()
    userId = update.effective_user.id
    if not conn:
        updater.bot.send_message(userId, "Erro na conexão ao banco de dados")
        return
    logging.info("Acionado o disparo de mensagens URGENTES - "+datetime.now().strftime('%d/%m/%Y %H:%M'))
    cursor = conn.cursor()
    dataAtual = datetime.today()
    cursor.execute('Select Mensagem from AvisosUrgentes Where DataEnvio Is Null')    
    mensagens = cursor.fetchall()
    if not mensagens:
        msgErro = "Não há mensagem a ser enviada"
        logging.info(msgErro) 
        updater.bot.send_message(userId, msgErro)
        return #não há mensagens a serem enviadas
    msgCofis = ""
    for mensagem in mensagens:
        if msgCofis!="":
            msgCofis = msgCofis+";\n"
        msgCofis = msgCofis+mensagem[0]
    if msgCofis!="":
        msgCofis = "Mensagem URGENTE Cofis:\n"+msgCofis+"."  
        if ambiente=="TESTE":
            msgCofis = msgCofis + "\nAmbiente: "+ambiente
    else:
        msgErro = "Não há mensagem a ser enviada (2)"
        logging.info(msgErro) 
        updater.bot.send_message(userId, msgErro)        
        return
    comando = "Select idTelegram from Usuarios Where Saida Is Null and idTelegram Is Not Null and idTelegram<>0 and Adesao Is Not Null"
    cursor.execute(comando)
    usuarios = cursor.fetchall()
    totalMsg = 0
    msgDisparadas = 0
    for usuario in usuarios: #percorremos os usuários ativos Telegram
        updater.bot.send_message(usuario[0], text=msgCofis)   
        totalMsg+=1
        msgDisparadas+=1
        if msgDisparadas>=30:
            msgDisparadas = 0
            time.sleep(1) #a cada 30 mensagens, dormimos um segundo (limitação do Bot é 30 por seg - TESTE) 
    msg = "Total de usuários para os quais foi enviada a mensagem (AvisoUrgente) no ambiente "+ambiente+": "+str(totalMsg)
    logging.info(msg) 
    updater.bot.send_message(userId, msg)     
    updater.bot.send_message(userId, "Mensagem que foi enviada para cada usuário:\n'"+msgCofis+"'")  
    try:
        comando = "Update AvisosUrgentes Set DataEnvio=%s Where DataEnvio Is Null"
        cursor.execute(comando, (dataAtual,))
        conn.commit()
    except:
        msgErro = "Erro ao atualizar a tabela de AvisosUrgentes - datas de envio ficaram em branco. Cuidado para não reenviar."
        logging.info(msgErro)
        updater.bot.send_message(userId, msgErro)         
        conn.rollback()
    return

def disparaMensagens(): #avisos diários (produção) ou de hora em hora (teste) contendo os alertas e mensagens da Cofis
    global updater, termina, ambiente
		
    try:
        server = smtplib.SMTP('INETRFOC.RFOC.SRF: 25') #servidor de email Notes
    except:
        logging.info("Erro na criação do servidor SMTP (disparaMensagens")
        server = None        
    conn = conecta()
    if not conn:
        return
    logging.info("Acionado o disparo de mensagens - "+datetime.now().strftime('%d/%m/%Y %H:%M'))
    cursor = conn.cursor(buffered=True)
    dataAtual = datetime.today().date()
    cursor.execute('Select Mensagem from MensagensCofis Where Data=%s', (dataAtual,))    
    mensagens = cursor.fetchall()
    msgCofis = ""
    for mensagem in mensagens:
        if msgCofis!="":
            msgCofis = msgCofis+";\n"
        msgCofis = msgCofis+mensagem[0]
    if msgCofis!="":
        msgCofis = "Mensagens Cofis:\n"+msgCofis+"."   #todos os usuários receberão essa mensagem na data informada pela Cofis, com ou sem alertas do dia 
    comando = "Select idTelegram, CPF, d1, d2, d3, email from Usuarios Where Adesao Is Not Null and Saida Is Null and idTelegram Is Not Null and idTelegram<>0"
    cursor.execute(comando)
    usuarios = cursor.fetchall()
    totalMsg = 0
    msgDisparadas = 0
    tdpfsAvisadosUpdate = set()
    tdpfsAvisadosInsert = set()
    cabecalho = "Alertas do dia (TDPF | Dias Restantes):" 
    for usuario in usuarios: #percorremos os usuários ativos Telegram
        if termina: #programa foi informado de que é para encerrar (quit)
            if server:
                server.quit()
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
        email = usuario[5]
        #selecionamos os TDPFs do usuário em andamento e monitorados (ativos) pelo serviço
        comando = """
                Select TDPFS.Numero, Supervisor, TDPFS.Codigo
                from CadastroTDPFs, TDPFS, Alocacoes, Fiscais
                Where Fiscais.CPF=%s and CadastroTDPFs.Fiscal=Fiscais.Codigo and CadastroTDPFs.TDPF=TDPFS.Codigo and 
                CadastroTDPFs.TDPF=Alocacoes.TDPF and  
                CadastroTDPFs.Fiscal=Alocacoes.Fiscal and TDPFS.Encerramento Is Null and 
                CadastroTDPFs.Fim Is Null and Alocacoes.Desalocacao Is Null
                Order by TDPFS.Numero
                """
        cursor.execute(comando, (cpf,))        
        fiscalizacoes = cursor.fetchall()
        comandoCiencias = "Select Data from Ciencias Where TDPF=%s Order By Data DESC"
        comandoAtividades = "Select Atividade, Vencimento, Inicio from Atividades Where TDPF=%s and Vencimento>=%s and Termino Is Null Order by Inicio, Vencimento"
        if fiscalizacoes:
            for fiscalizacao in fiscalizacoes: #percorremos os TDPFs MONITORADOS do usuário
                if termina: #foi solicitado o término do bot
                    return  
                chaveTdpf = fiscalizacao[2]          
                tdpf = fiscalizacao[0] 
                tdpfFormatado = formataTDPF(tdpf)
                supervisor = fiscalizacao[1]
                cursor.execute(comandoCiencias, (chaveTdpf,)) #buscamos as ciências do TDPF
                ciencias = cursor.fetchall() #buscamos todas por questões técnicas do mysqlconnector
                if len(ciencias)>0:       
                    dataCiencia = ciencias[0][0].date()  #só é necessária a última (selecionamos por ordem descendente)      
                    prazoRestante = (dataCiencia+timedelta(days=60)-dataAtual).days                
                    if prazoRestante==d1 or prazoRestante==d2 or prazoRestante==d3:
                        if len(listaUsuario)==0:
                            listaUsuario = cabecalho                       
                        if supervisor=='S':    
                            tdpfFormatado2 = tdpfFormatado + '* (S)'
                        else:
                            tdpfFormatado2 = tdpfFormatado + '*'
                        listaUsuario = listaUsuario+"\n\n*"+tdpfFormatado2+ " | "+str(prazoRestante)+" (a)"
           
                #buscamos as atividades do TDPF    
                cursor.execute(comandoAtividades, (chaveTdpf, dataAtual))
                atividades = cursor.fetchall()
                for atividade in atividades:
                    prazoRestante = (atividade[1].date()-dataAtual).days
                    if prazoRestante==0 or prazoRestante==d3: #para atividade, alertamos só no d3 (o menor) e no dia do vencimento (prazo restante == 0)
                        if len(listaUsuario)==0:
                            listaUsuario = cabecalho
                        listaUsuario = listaUsuario+"\n\n*"+tdpfFormatado+"* | "+str(prazoRestante)+" (b)"
                        #listaUsuario = listaUsuario+"\nAtividade: "+atividade[0]+"; Início: "+atividade[2].strftime("%d/%m/%Y")
                        listaUsuario = listaUsuario+"\nAtividade: "+atividade[0]+"; Início: "+atividade[2].strftime("%d/%m/%Y")+"; Vencimento: "+atividade[1].strftime("%d/%m/%Y")

        #selecionamos as datas de vencimento dos TDPFs em que o usuário está alocado, mesmo que não monitorados
        comando = """
                Select TDPFS.Numero, TDPFS.Vencimento, Supervisor, TDPFS.Codigo, Fiscais.Codigo from TDPFS, Alocacoes, Fiscais
                Where Fiscais.CPF=%s and Alocacoes.Fiscal=Fiscais.Codigo and TDPFS.Codigo=Alocacoes.TDPF and TDPFS.Encerramento Is Null and 
                Alocacoes.Desalocacao Is Null
                """        
        cursor.execute(comando, (cpf,))     
        comandoVencimento = "Select Codigo, Data from AvisosVencimento Where TDPF=%s and Fiscal=%s"                   
        tdpfUsuarios = cursor.fetchall()
        for tdpfUsuario in tdpfUsuarios: #percorremos os TDPFs do usuário para ver suas datas de vencimento (TODOS em andamento no qual o usuário esteja atualmente alocado)
            vencimento = tdpfUsuario[1]
            tdpf = tdpfUsuario[0]
            tdpfFormatado = formataTDPF(tdpf)
            supervisor = tdpfUsuario[2]
            chaveTdpf = tdpfUsuario[3]
            chaveFiscal = tdpfUsuario[4]
            if vencimento: #não deve ser nulo, mas garantimos ...
                vencimento = vencimento.date()
                prazoVenctoTDPF = (vencimento-dataAtual).days
                if not (1<=prazoVenctoTDPF<=15): #se não estiver próximo do vencimento (1 a 15 dias), prosseguimos (pulamos)
                    continue 
                cursor.execute(comandoVencimento, (chaveTdpf, chaveFiscal))  #buscamos as datas de aviso para o CPF e TDPF  
                avisos = cursor.fetchall()
                if len(avisos)==0: #não há avisos para o tdpf (este cpf)
                    avisou = None
                else:
                    avisou = avisos[0][1].date() #só retorna uma linha para cada tdpf/cpf      
                    codigo = avisos[0][0] #chave primária do registro        
                #não avisamos do vencimento de TDPF recentemente avisado (prazo: 7 dias - depende da carga)
                if not avisou:
                    podeAvisar = True
                    tdpfsAvisadosInsert.add(str(chaveTdpf).rjust(15,"0")+str(chaveFiscal).rjust(12,"0"))     
                else:
                    if (avisou+timedelta(days=7))<dataAtual: #vai depender da periodicidade da extração e carga - aqui estamos considerando 7 dias
                        podeAvisar = True
                        tdpfsAvisadosUpdate.add(codigo)                        
                    else:
                        podeAvisar = False                      
                if podeAvisar: #verificar este prazos quando for colocar em produção
                    if len(listaUsuario)==0:
                        listaUsuario = "Alertas do dia (TDPF | Dias Restantes):"
                    if supervisor=='S':    
                        tdpfFormatado2 = tdpfFormatado + ' (S)'
                    else:
                        tdpfFormatado2 = tdpfFormatado                         
                    listaUsuario = listaUsuario+"\n\n*"+tdpfFormatado2+ "* | "+str(prazoVenctoTDPF)+" (c)"                 

        if len(listaUsuario)>0 or msgCofis!="":
            if len(listaUsuario)>0:
                listaUsuario = listaUsuario+"\n\n(a) P/ recuperação da espontaneidade tributária."
                listaUsuario = listaUsuario+"\n(b) P/ vencimento da atividade."            
                listaUsuario = listaUsuario+"\n(c) P/ vencimento do TDPF no Ação Fiscal - pode ser inferior à data do Ação Fiscal, pois as informações do serviço se baseiam"
                listaUsuario = listaUsuario+" na data de distribuição, que pode ter ocorrido antes da assinatura e emissão do TDPF."
            if msgCofis!="":
                if len(listaUsuario)>0:
                    listaUsuario = listaUsuario+"\n\n"
                listaUsuario = listaUsuario+msgCofis
            if ambiente=="TESTE":
                listaUsuario = listaUsuario+"\n\nAmbiente: "+ambiente
            logging.info("Disparando mensagem para "+cpf)
            updater.bot.send_message(usuario[0], text=limpaMarkdown(listaUsuario), parse_mode= 'MarkdownV2')  
			#enviamos e-mail também, se houver um na tabela
            if email!=None and server!=None:
                email = email.strip()
                if email!="":
                    # create message object instance
                    msg = MIMEMultipart()               
                    # setup the parameters of the e-mail message
                    msg['From'] = "botespontaneidade@rfb.gov.br"
                    msg['Subject'] = "BotEspontaneidade - Avisos e Alertas do Dia"                    
                    msg['To'] = email			
					# add in the message body
                    msg.attach(MIMEText(listaUsuario.replace('*', ''), 'plain'))				
					# send the message via the server.
                    try:
                        server.sendmail(msg['From'], msg['To'], msg.as_string())
                    except:
                        logging.info("Erro no envio de email com os avisos do dia - "+email)							
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
        tupla = (int(tdpfCpf[:15]), int(tdpfCpf[15:]), dataAtual) #ao adicionar, o código do TDPF ocupou 15 posições e a chave do fiscal, 12
        lista.append(tupla)
    if len(lista)>0:
        logging.info("Inserção:")
        logging.info(lista)
        comando = "Insert Into AvisosVencimento (TDPF, Fiscal, Data) Values (%s, %s, %s)"
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
    if server:
        server.quit()  

    #buscamos todos os TDPFs para os quais não foram registradas nenhuma data de ciência após 30 dias (exatos) de sua distribuição
    # e avisamos o supervisor
    comando = """
              Select Distinctrow Fiscais.CPF, TDPFS.Numero, Usuarios.idTelegram, Supervisores.Equipe, TDPFS.Codigo
              From TDPFS, Usuarios, Supervisores, Fiscais
              Where Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Emissao=cast((now() - interval 30 day) as date) 
              and TDPFS.Encerramento Is Null and Supervisores.Fiscal=Fiscais.Codigo and Fiscais.CPF=Usuarios.CPF and Usuarios.idTelegram Is Not Null and 
              Usuarios.idTelegram<>0 and Usuarios.Adesao Is Not Null and Usuarios.Saida Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)
              Order By Fiscais.CPF, Supervisores.Equipe, TDPFS.Numero
              """
    consultaFiscal = "Select Fiscais.Nome From TDPFS, Alocacoes, Fiscais Where TDPFS.Codigo=%s and TDPFS.Codigo=Alocacoes.TDPF and Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.Desalocacao Is Null Order By Alocacoes.Alocacao"
    cursor.execute(comando)
    linhas = cursor.fetchall() 
    msg = ""  
    cpfAnt = "" 
    userId = 0 
    cabecalho = "TDPFs sem informação de início (ciência) do procedimento fiscal há 30 dias:"
    equipe = ""
    for linha in linhas:
        if cpfAnt=="":
            cpfAnt = linha[0]
        if linha[0]!=cpfAnt:
            if msg!="":
                msg = cabecalho+msg
                if ambiente=="TESTE":
                    msg = msg+"\n"+"Ambiente: "+ambiente                
                updater.bot.send_message(userId, text=msg) 
                msg = ""
            equipe = ""    
            cpfAnt = linha[0]    
        if linha[3]!=equipe:
            equipe = linha[3]
            msg = msg+"\nEquipe " + equipe[:7]+"."+equipe[7:11]+"."+equipe[11:] + ":"    
        chaveTdpf = linha[4]    
        cursor.execute(consultaFiscal, (chaveTdpf,))              
        fiscal = cursor.fetchone()
        if fiscal==None:
            nomeFiscal = "ND"
        else:
            nomeFiscal = fiscal[0]
        msg = msg + "\n  "+formataTDPF(linha[1])+" ("+nomeFiscal+")"
        userId = linha[2]    
    if msg!="":
        msg = cabecalho+msg
        if ambiente=="TESTE":
            msg = msg+"\n"+"Ambiente: "+ambiente        
        updater.bot.send_message(userId, text=msg)  

    #buscamos todos os TDPFs que estão recuperando a espontaneidade em 15 dias exatos e avisamos o supervisor
    comando = """
              Select Distinctrow Fiscais.CPF, TDPFS.Numero, Usuarios.idTelegram, Supervisores.Equipe, TDPFS.Codigo
              From TDPFS, Usuarios, Supervisores, Fiscais 
              Where Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Null and
              Supervisores.Fiscal=Fiscais.Codigo and Fiscais.CPF=Usuarios.CPF and Usuarios.idTelegram Is Not Null and Usuarios.idTelegram<>0 and Usuarios.Adesao Is Not Null and 
              Usuarios.Saida Is Null and TDPFS.Codigo in (Select TDPF from Ciencias Where Data Is Not Null)
              Order By Fiscais.CPF, Supervisores.Equipe, TDPFS.Numero
              """         
    cursor.execute(comando)
    linhas = cursor.fetchall() 
    msg = ""  
    cpfAnt = "" 
    userId = 0 
    consulta = "Select Data, Documento from Ciencias Where TDPF=%s Order By Data DESC"    
    cabecalho = "TDPFs em andamento com espontaneidade tributária sendo recuperada em 15 dias (ciência em "+(dataAtual-timedelta(days=45)).strftime("%d/%m/%Y")+"):"
    equipe = ""
    for linha in linhas:
        tdpf = linha[1]               
        if cpfAnt=="":
            cpfAnt = linha[0]
        if linha[0]!=cpfAnt:
            if msg!="":
                msg = cabecalho+msg
                if ambiente=="TESTE":
                    msg = msg+"\n"+"Ambiente: "+ambiente                
                updater.bot.send_message(userId, text=msg) 
                msg = ""
            equipe = ""    
            cpfAnt = linha[0]    
        if linha[3]!=equipe:
            equipe = linha[3]
        chaveTdpf = linha[4]
        cursor.execute(consultaFiscal, (chaveTdpf,))              
        fiscal = cursor.fetchone()
        if fiscal==None:
            nomeFiscal = "ND"
        else:
            nomeFiscal = fiscal[0]        
        cursor.execute(consulta, (chaveTdpf,))
        cienciaReg = cursor.fetchone() #buscamos a última data de ciência do TDPF
        if cienciaReg:
            if len(cienciaReg)>0:
                dataCiencia = cienciaReg[0].date()
                prazoRestante = (dataCiencia+timedelta(days=60)-dataAtual).days                
                if prazoRestante==15:
                    if msg=="":
                        msg = msg+"\nEquipe " + equipe[:7]+"."+equipe[7:11]+"."+equipe[11:] + ":"                                
                    msg = msg + "\n  "+formataTDPF(tdpf)+" ("+nomeFiscal+")"
                    if cienciaReg[1]: #documento informado
                        msg = msg +" - "+cienciaReg[1]
        userId = linha[2]               
    if msg!="":
        msg = cabecalho+msg
        if ambiente=="TESTE":
            msg = msg+"\n"+"Ambiente: "+ambiente
        updater.bot.send_message(userId, text=msg) 

    conn.close()              
    return


def disparador():
    global termina, dirLog, sistema, ambiente, diaAtual
    logging.info("Disparador (thread) iniciado ...")
    while not termina:
        schedule.run_pending() 
        logging.info("Disparador (thread) indo 'dormir'")
        time.sleep(60*60) #dorme por 1 h
        dia = datetime.now().date()
        if ambiente!='TESTE' and diaAtual!=dia:
            diaAtual = dia
            #a cada dia inicia um arquivo de log diferente
            logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Bot'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO, force=True)       
    return 

def conecta():
    global MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, hostSrv
    try:
        #logging.info("BD: "+MYSQL_DATABASE)
        #logging.info(MYSQL_PASSWORD)
        #logging.info("User: "+MYSQL_USER)

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
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234")
token = os.getenv("TOKEN", "ERRO")
ambiente = os.getenv("AMBIENTE", "TESTE")
senhaAvisoUrgente = os.getenv("AVISOURGENTE", "760714")
logging.info("Ambiente: "+ambiente)
if token=="ERRO":
    logging.error("Token do Bot Telegram não foi fornecido.")
    print("Token não informado")
    sys.exit(1)

logging.info("Conectando ao servidor de banco de dados ...")
conn = conecta() #não será utilizada - apenas conectamos para ver se está ok
if not conn:
    sys.exit(1)
logging.info("Conexão efetuada com sucesso ao MySql!")
logging.info("BD: "+MYSQL_DATABASE)
#logging.info(MYSQL_PASSWORD)
logging.info("User: "+MYSQL_USER)
conn.close() #cada função criará sua conexão (aqui é só um teste para inicializarmos o Bot)
#dias de antecedência padrão para avisar
d1padrao = 30
d2padrao = 20
d3padrao = 5

atividadeTxt = {} #guarda o tdpf e o prazo de uma atividade, pendente a informação do texto de sua descrição (id: [tdpf, data])

cienciaTxt = {} #guarda o tdpf e a data de ciência, pendente a informação do texto da descrição do documento (id: [tdpf, data])

pendencias = {} #indica que próxima função deve ser chamada para analisar entrada de dados

#encaminha a pendência para a função adequada para tratar o input do usuário
dispatch = { 'registra': registra, 'ciencia': ciencia, 'prazos': prazos, 'acompanha': acompanha,
             'anulaCiencia': anulaCiencia, 'fim': fim, 'email': cadastraEMail,
             'atividade': atividade, 'anulaAtividade': anulaAtividade, 
             'atividadeTexto': atividadeTexto, 'cienciaTexto': cienciaTexto, 'envioChave': envioChave, 
             'mostraSuperv': mostraSupervisionados, 'informaTerminoAtividade': terminoAtividade,
             'supervisorEspontaneidade': mostraSupervisorEspontaneidade, 'informaHorasAtividade': informaHorasAtividade}
textoRetorno = "\nEnvie /menu para retornar ao menu principal"
updater = None #para ser acessível ao disparador de mensagens
#schedule.every().day.at("07:30").do(disparaMensagens)
if ambiente=="TESTE":
    schedule.every(60).minutes.do(disparaMensagens) #deixamos enviar msgs a cada 1 h no ambiente de testes
else:
	schedule.every().day.at("07:30").do(disparaMensagens) #uma vez por dia - produção

termina = False
diaAtual = datetime.now().date() #será utilizado para criar um arquivo de Log p/ cada dia
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
       