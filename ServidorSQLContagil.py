# -*- coding: utf-8 -*-
"""
Created on Tue Jul 21 17:27:16 2020

@author: 53363833172
"""

from __future__ import unicode_literals
from datetime import datetime, timedelta
import time
import sys
import os
import logging   
import mysql.connector
from mysql.connector import errorcode
import re
import socket
import threading
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import os
from random import randint
import calendar

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
import smtplib

#função para o envio de e-mail - copiado do Bot_Telegram.py
def enviaEmail(email, texto, assunto, arquivo=None):
    try:
        #server = smtplib.SMTP('INETRFOC.RFOC.SRF: 25') #servidor de email Notes
        #pass
        server = smtplib.SMTP('exchangerfoc.rfoc.srf: 25')
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
##########################################################################################

def descriptografa(msgCripto, addr, c):
    global chavesCripto
    try:
        decrypted = chavesCripto[segmentoIP(addr)][1].decrypt(msgCripto).decode("utf-8")
    except:
        #se não deu certo, esperamos um tempo para ver se chega o restante da mensagem (pode ter vindo só uma parte)
        try:
            msg = c.recv(1024)
            if msg==None or msg==b"":
                return "000000000A"
        except: #time out
            return "000000000A" #só uma mensagem dummy para demonstrar o erro quando não for possível descriptografar (não chegou o restante)
        #chegou - tentamos descriptografar agora com o restante da mensagem
        try:
            decrypted = chavesCripto[segmentoIP(addr)][1].decrypt(msgCripto+msg).decode("utf-8")
        except:
            return "000000000A" #só uma mensagem dummy para demonstrar o erro quando não for possível descriptografar
    #decrypted = decryptor.decrypt(chaveCripto).decode("utf-8")
    return decrypted    

def getAlgarismos(texto): #retorna apenas os algarismos de uma string
    limpo = ""
    for car in texto:
        if car.isdigit():
            limpo = limpo + car
    return limpo 

#verifica se um CPF é válido
def validaCPF(cpfPar):
#The MIT License (MIT) Copyright (c) 2015 Derek Willian Stavis
    if cpfPar==None:
        return False
    if not 'STR' in str(type(cpfPar)).upper() and not "UNICODE" in str(type(cpfPar)).upper():
        return False
    cpf = getAlgarismos(cpfPar)
    if len(cpf)!=11:
        return False

    if cpf in [s * 11 for s in [str(n) for n in range(10)]]:
        return False

    calc = lambda t: int(t[1]) * (t[0] + 2)
    d1 = (sum(map(calc, enumerate(reversed(cpf[:-2])))) * 10) % 11
    d2 = (sum(map(calc, enumerate(reversed(cpf[:-1])))) * 10) % 11
    if d1==10:
        d1 = 0
    if d2==10:
        d2 = 0    
    return str(d1) == cpf[-2] and str(d2) == cpf[-1]

#transforma uma data string de dd/mm/yyyy para yyyy/mm/dd para fins de consulta, inclusão ou atualização no BD SQL
#se o BD esperar a data em outro formato, basta alterarmos aqui
def converteAMD(data):
    return data[6:]+"/"+data[3:5]+"/"+data[:2]


def verificaEMail(email): #valida o e-mail se o usuário informou um completo
    regex1 = '^[a-zA-Z0-9]+[\._]?[a-zA-Z0-9\.\-]+[@]\w+[.]\w{2,3}$'
    regex2 = '^[a-zA-Z0-9]+[\._]?[a-zA-Z0-9\.\-]+[@]\w+[.]\w+[.]\w{2,3}$'  

    if(re.search(regex1,email)):  
        return True   
    elif(re.search(regex2,email)):  
        return True
    else:  
        return False

def dataTexto(data):
    if data==None:
        return "00/00/0000"
    else:
        try:
            return data.strftime("%d/%m/%Y")     
        except:
            return "00/00/0000"

def digitoVerificadorModulo11(codigo, dv1QuandoModulo0):
    soma = 0
    peso = 2
    i = len(codigo)-1
    while i>=0:
        soma += int(codigo[i:i+1]) * peso
        peso+=1
        i-=1
    modulo = soma % 11
    if (11 - modulo)==10:
        return "0"
    if (11 - modulo)==11:
        return "1" if dv1QuandoModulo0 else "0"
    return str(11 - modulo)
                        
def verificaDVDCC(dcc): #só 17 dígitos
    primeiroDV = dcc[-2:-1]
    segundoDV = dcc[-1:]
    dccSemDV = dcc[:15]
    primDVCalculado = digitoVerificadorModulo11(dccSemDV, True)
    if primDVCalculado==primeiroDV:
        segDVCalculado = digitoVerificadorModulo11(dccSemDV + primDVCalculado, True)
        if segDVCalculado==segundoDV:
            return True
    return False

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

def conectaRaw():
    global MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, hostSrv
    try:
        #logging.info("BD: "+MYSQL_DATABASE)
        #logging.info(MYSQL_PASSWORD)
        #logging.info("User: "+MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv,
                                    database=MYSQL_DATABASE, raw=True)
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

def enviaRespostaSemFechar(resposta, c):
    #logging.info(resposta)    
    tam = str(len(resposta)).rjust(5,"0").encode('utf-8')    
    resposta = resposta.encode('utf-8')      
    try:    
        c.sendall(tam+resposta)
        return 
    except:
        logging.info("Erro ao enviar a resposta - exceção - "+str(resposta))
    return

def enviaResposta(resposta, c):
    enviaRespostaSemFechar(resposta, c)
    c.close()        
    return

def isDate(data): #verifica se a string é uma data válida
    if data==None:
        return False
    if len(data)!=10: #exige no formato dd/mm/aaaa
        return False
    # faz o split e transforma em números
    try:
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

def atualizaTentativas(codigo, tentativas, conn): #incrementa o numero de tentativas em caso de erro
    if tentativas==None:
        tentativas = 1
    else:
        tentativas+=1
    cursor = conn.cursor(buffered=True)
    comando = "Update Usuarios Set Tentativas=%s Where Codigo=%s"
    try:
        cursor.execute(comando, (tentativas, codigo))
        conn.commit()   #FALTA CONTROLE DE ERROS
    except:
        conn.rollback() #não há o que fazer
    return

def zeraTentativas(codigo, conn): #zera as tentativas de utilizar a senha em caso de acerto
    cursor = conn.cursor(buffered=True)
    comando = "Update Usuarios Set Tentativas=0 Where Codigo=%s"
    try:
        cursor.execute(comando, (codigo,))
        conn.commit()    #FALTA CONTROLE DE ERROS
    except:
        conn.rollback() #não há o que fazer
    return 

def eliminaTags(texto):
    tags = 0
    if texto==None:
        return ""
    textoLimpo = ""
    for j in range(len(texto)):
        if texto[j:j+1]=="<":
            tags+=1
            continue
        if texto[j:j+1]==">":
            tags-=1
            continue
        if tags==0:
            textoLimpo = textoLimpo + texto[j:j+1]
    return textoLimpo

def verificaAlocacao(conn, cpf, tdpf): #verifica se o fiscal (cpf) está alocado ao TDPF em andamento - retorna tb o nome do fiscalizado
    cursor = conn.cursor(buffered=True)
    comando = """Select TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao 
                 from TDPFS, Alocacoes, Fiscais 
                 Where TDPFS.Numero=%s and Fiscais.CPF=%s and Fiscais.Codigo=Alocacoes.Fiscal and TDPFS.Codigo=Alocacoes.TDPF and 
                 TDPFS.Encerramento Is Null and Alocacoes.Desalocacao Is Null"""
    cursor.execute(comando, (tdpf, cpf))
    row = cursor.fetchone()  
    if not row:
        return False, None
    if len(row)==0:
        return False, None
    return True, row[0]

def verificaSupervisao(conn, cpf, tdpf): #verifica se o fiscal (cpf) é supervisor da equipe do fiscal responsável pelo TDPF - retorna tb o nome do fiscalizado
    cursor = conn.cursor(buffered=True)
    comando = """Select TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao 
                 from TDPFS, Supervisores, Fiscais 
                 Where TDPFS.Numero=%s and TDPFS.Grupo=Supervisores.Equipe and Fiscais.CPF=%s and Fiscais.Codigo=Supervisores.Fiscal and Supervisores.Fim Is Null"""
    cursor.execute(comando, (tdpf, cpf))
    row = cursor.fetchone()  
    if not row:
        return False, None
    if len(row)==0:
        return False, None
    return True, row[0] 

def tdpfMonitoradoCPF(conn, tdpf, cpf): #verifica se o TDPF está sendo monitorado e se tal monitoramento está ativo
    cursor = conn.cursor(buffered=True)
    comando = """Select CadastroTDPFs.Codigo, CadastroTDPFs.Fim from CadastroTDPFs, Fiscais, TDPFS 
                 Where TDPFS.Numero=%s and Fiscais.CPF=%s and CadastroTDPFs.TDPF=TDPFS.Codigo and CadastroTDPFs.Fiscal=Fiscais.Codigo"""
    cursor.execute(comando, (tdpf, cpf))
    row = cursor.fetchone()
    tdpfMonitorado = False
    monitoramentoAtivo = None
    chave = None
    if row:
        if len(row)>0:
            tdpfMonitorado = True  
            chave = row[0]
            fim = row[1]
            if fim==None:
                monitoramentoAtivo = True
            else:
                monitoramentoAtivo = False
    return tdpfMonitorado, monitoramentoAtivo, chave #indica se o tdpf está sendo monitorado; se True, o segundo retorno indica se tal monitoramento está ativo e o terceiro a chave do registro     

def segmentoIP(addr):
    global ambiente
    return sum(map(int, addr.split("."))) % (10 if ambiente=="TESTE" else 20) #10 segmentos (chaves) para ambiente de testes e 20 para ambiente de produção

def geraChaves(addr): #gera a chave (par) do usuário, coloca no dicionário de validades e retorna a chave pública para ser encaminhada a ele
    global chavesCripto
    keyPair = RSA.generate(2048)
    decryptor = PKCS1_OAEP.new(keyPair)        
    chavesCripto[segmentoIP(addr)] = [keyPair, decryptor, datetime.now()+timedelta(hours=1)] #validade de 1h para este conjunto de chaves
    #pubKey = keyPair.publickey()
    return #pubKey
    #print(f"Public key:  (n={hex(pubKey.n)}, e={hex(pubKey.e)})")
    #pubKeyPEM = pubKey.exportKey()   

def inicializaChaves():
    global chavesCripto, ambiente   
    for i in range(10 if ambiente=="TESTE" else 20): #geramos 20 chaves se for ambiente de produção e 10 se for ambiente de testes
        keyPair = RSA.generate(2048)
        decryptor = PKCS1_OAEP.new(keyPair)        
        chavesCripto[i] = [keyPair, decryptor, datetime.now()+timedelta(hours=1)] #cada chave tem validade de uma hora
    return

def estaoChavesValidas(addr):
    global chavesCripto
    if chavesCripto==None:
        return False #não há chave a ser revalidada
    if chavesCripto[segmentoIP(addr)][2]<datetime.now():
        return False
    else:
        return True

def buscaTipoOrgao(orgao, cursor):
    if orgao==0 or orgao==None:
        return "L"
    comando = "Select Tipo from Orgaos Where Codigo=%s"
    cursor.execute(comando, (orgao,))
    row = cursor.fetchone()
    if row==None or len(row)==0:
        return"L"
    else:
        orgaoResp = row[0]
        if orgaoResp=="" or orgaoResp==None:
            orgaoResp = "L"  
        return orgaoResp 

def consultaPontuacao(cursor, chaveTdpf, tipoOrgaoUsuario, pediuParametros=False, bSupervisor=False):
    #pontuação de cada tributo (código) por porte [p, m, g]
    pontosTribPJ = {2141: [111, 131, 176], 2096: [111, 131, 176], 694: [132, 174, 247], 1167: [131, 155, 155], 1011: [130, 148, 159],
                    221: [132, 174, 247], 238: [87, 87, 151], 8103: [67, 101, 148], 3880: [137, 177, 210], 6121: [137, 177, 210], 3333: [122, 122, 122]} #alterado Portaria 06/2021 - igualou pontos IRPJ/CSLL

    pontosTribPF = {2141: [40, 40, 40], 2096: [40, 40, 40], 210: [75, 75, 105]} 
    #para PJ, se não estiver na relação de pontosTrib, pega essa aí como pontuação e sempre como base do multiplicado para cada porte [p, m, g]
    pontosOutrosPJ = [118, 158, 194]

    #para PF, se não estiver na relação de pontosTrib, pega essa aí como pontuação e sempre como base do multiplicado para cada porte [p, m, g]
    pontosOutrosPF = [68, 68, 85]  #preenchi a posição do meio com o mesmo valor da primeira (demais) para não correr riscos

    #acréscimos (percentuais) na pontuação base (pontosTrib ou, subsidiariamente, pontosOutrosPF/PJ) de acordo com algum tributo a mais programado (art. 10, par. 2º)
    acrescimos = {2141: 0.4, 2096: 0.4, 1011: 0.4, 210: 0.4, 221: 0.4, 694: 0.4, 8103: 0.4,  3880: 0.4, 6121: 0.4, 1167: 0.1, 238: 0.1, 9984: 0.1, 9985: 0.1} #alterado Portaria 06/2021 (694, 9984, 9985)
             
    comando = "Select Porte, Encerramento, NI, Acompanhamento from TDPFS Where TDPFS.Codigo=%s"
    cursor.execute(comando, (chaveTdpf,))
    row = cursor.fetchone()
    if not row or len(row)==0: #o TDPF não existe (??)
        return  None
    porte = row[0]        
    if porte==None:
        porte = "DEM"  
    if row[1]==None:
        encerrado = "N"
    else:
        encerrado = "S" 
    ni = row[2]
    #verificamos se o contribuinte está sujeito a acompanhamento especial pela Comac
    acompanhamento = "N"
    if row[3]!=None:
        if row[3]=="S":
            acompanhamento = "S"
    #contamos as operações do TDPF
    comando = "Select Count(Distinct Operacao) from Operacoes Where TDPF=%s"
    cursor.execute(comando, (chaveTdpf,))
    row = cursor.fetchone()
    if not row or len(row)==0:
        qtdeOperacoes = 0
    else:
        qtdeOperacoes = row[0]   
    #temos que ver se há operações de PIS/Cofins programados e diminuir, da quantidade obtida acima, a quantidade de programação de um tributo destes que for menor
    comando = """Select Count(Distinct Operacoes.Operacao) from Operacoes, OperacoesFiscais, Tributos 
                    Where TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo and Operacoes.Tributo=Tributos.Codigo and Tributos.Tributo=%s"""
    cursor.execute(comando, (chaveTdpf, 3880)) #PIS
    row = cursor.fetchone()
    qtdePis = 0
    if row:
        qtdePis = row[0]
    cursor.execute(comando, (chaveTdpf, 6121)) #Cofins
    row = cursor.fetchone()        
    qtdeCofins = 0
    if row:
        qtdeCofins = row[0]
    qtdeOperacoes = qtdeOperacoes - min(qtdePis, qtdeCofins)
    #contamos os anos programados do TDPF     
    comando = "Select Min(PeriodoInicial), Max(PeriodoFinal) from Operacoes Where TDPF=%s"
    cursor.execute(comando, (chaveTdpf,))
    row = cursor.fetchone()
    if not row or len(row)==0:
        qtdeAnos = 0
    else:
        if row[1]==None or row[0]==None:
            qtdeAnos = 1
        else:    
            qtdeAnos = int(row[1].strftime("%Y"))-int(row[0].strftime("%Y"))+1             

    #obtemos os parâmetros internos e já começamos a calcular os pontos
    #para calcular, estabelecemos a posição do porte do fiscalizado na lista de pontos que utilizaremos
    if "DEM" in porte:
        posicao = 0
    elif "DIF" in porte:
        posicao = 2
    else:
        posicao = 1
    #buscamos todos os tributos
    comando = """Select Tributos.Tributo, OperacoesFiscais.Operacao From Operacoes, OperacoesFiscais, Tributos 
                    Where Operacoes.TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo and Operacoes.Tributo=Tributos.Codigo"""
    cursor.execute(comando, (chaveTdpf,))
    rows = cursor.fetchall()
    pontos = float(0)
    tributoPrincipal = 0
    if len(ni)==18: #PJ
        pontosTrib = pontosTribPJ
        baseMultiplicador = pontosOutrosPJ[posicao]
    else:
        pontosTrib = pontosTribPF
        baseMultiplicador = pontosOutrosPF[posicao]
    #vemos qual tributo programado oferece a maior pontuação
    listaTributos = []
    operacao40111 = 'N'
    for tributo in rows:
        listaTributos.append(tributo[0])
        if tributo[1]==40111: #operação livro caixa em IRPF que justifica acréscimo da situação11
            operacao40111 = 'S'
        pontos2 = max(pontos, pontosTrib.get(tributo[0],[0,0,0])[posicao])
        if pontos2>pontos:
            pontos = pontos2
            tributoPrincipal = tributo[0]      
    acrescimo = float(0)            
    if pontos==0:
        pontos = baseMultiplicador #se não achou o tributo em pontosTrib, a pontuação fica sendo a subsidiária que depende do tipo da pessoa (F ou J)
    else:
        for tributo in rows: #se  achou o tributo em pontosTrib, pode haver um acréscimo por outro tributo programado
            if tributo[0]==tributoPrincipal or (tributo[0]==221 and tributoPrincipal==694) or (tributo[0]==694 and tributoPrincipal==221) \
                or (tributo[0]==3880 and tributoPrincipal==6121) or (tributo[0]==6121 and tributoPrincipal==3880): 
                #desde que não seja aquele que utilizamos em pontosTrib e nem IRPJ (221) com CSLL (694) e vice-versa ou PIS (3880) com Cofins (6121) e vice versa
                continue
            acrescimo = max(acrescimo, acrescimos.get(tributo[0],0))
    #print("Pontos Iniciais", pontos)
    pontosTribPrincipal = pontos
    #print("Acréscimo", acrescimo)
    pontos = pontos * (1.0+acrescimo)
    #vemos qual a operação programada tem o maior valor, independente do tributo
    comando = """Select OperacoesFiscais.Operacao, OperacoesFiscais.Valor 
                    from Operacoes, OperacoesFiscais 
                    Where Operacoes.TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo and OperacoesFiscais.Valor 
                    in (Select Max(Valor) from OperacoesFiscais, Operacoes 
                        Where Operacoes.TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo)"""
    cursor.execute(comando, (chaveTdpf, chaveTdpf))
    row = cursor.fetchone()
    if row:
        opPrincipal = row[0]
        multOp = row[1]
    else:
        multOp = 1
        opPrincipal = 0              

    pontos = pontos * float(multOp) #anexo único da Portaria Cofis 46/2020
    #print("Pontos", pontos)            
    #buscamos os parâmetros para cálculo do multiplicador (se existirem) e não foi pedido apenas o parâmetro
    if not pediuParametros:
        comando = """Select Arrolamentos, MedCautelar, RepPenais, Inaptidoes, Baixas, ExcSimples, SujPassivos, DigVincs, Situacao11, Interposicao,
                    Situacao15, EstabPrev1, EstabPrev2, Segurados, Prestadores, Tomadores, QtdePER, LancMuldi, Compensacao, CreditoExt
                    from Resultados Where TDPF=%s"""        
        cursor.execute(comando, (chaveTdpf,))
        row = cursor.fetchone()
    else:
        row = None
    if row or ((tipoOrgaoUsuario in ["N","R"] or bSupervisor) and not pediuParametros):
        if row: #não pediu parâmetros e houve a prestação das informações dos parâmetros
            arrolamentos = row[0]
            medCautelar = row[1]
            rffps = row[2]
            inaptidoes = row[3]
            baixas = row[4]
            excSimples = row[5]
            sujPassivos = row[6]
            digVincs = row[7]
            situacao11 = row[8]
            interposicao = row[9]
            situacao15 = row[10]
            estabPrev1 = row[11]
            estabPrev2 = row[12]
            segurados = row[13]
            prestadores = row[14]
            tomadores = row[15]
            qtdePER = row[16]            
            lancMuldi = row[17]
            compensacao = row[18]
            creditoExt = row[19]
        else: #não pediu parâmetros, não houve a prestação das informações dos parâmetros, mas é usuário regional ou nacional - calculamos os pontos com eles zerados
            arrolamentos = 0
            medCautelar = 'N'
            rffps = 0
            inaptidoes = 0
            baixas = 0
            excSimples = 0
            sujPassivos = 0
            digVincs = 0
            situacao11 = 'N'
            interposicao = 'N'
            situacao15 = 'N'
            estabPrev1 = 0
            estabPrev2 = 0
            segurados = 0
            prestadores = 0
            tomadores = 0
            qtdePER = 0            
            lancMuldi = 'N'
            compensacao = 'N'
            creditoExt = 'N'                
        parametros = str(arrolamentos).rjust(2,"0")+medCautelar+str(rffps).rjust(2,"0")+str(inaptidoes).rjust(2,"0")+str(baixas).rjust(2,"0")+str(excSimples).rjust(2,"0")
        parametros = parametros + str(sujPassivos).rjust(2,"0")+str(digVincs).rjust(3,"0")+situacao11+interposicao+situacao15
        parametros = parametros + str(estabPrev1).rjust(3,"0")+str(estabPrev2).rjust(2,"0")+str(segurados).rjust(4,"0")+str(prestadores).rjust(3,"0")+str(tomadores).rjust(3,"0")
        parametros = parametros + str(qtdePER).rjust(2,"0")+lancMuldi+compensacao+creditoExt
        multiplicador = float(0)
        multPar = [] #para informar por e-mail os parâmetros e respectivos pontos gerados
        #obtemos o multiplicador com os demais parâmetros
        if 0<arrolamentos<=2:
            multiplicador = 0.2*arrolamentos
            multPar.append("Arrolamentos: "+str(multiplicador)) #alterado Portaria 06/2021
        elif arrolamentos>2:
            multiplicador = 0.2+0.1*arrolamentos  
            multPar.append("Arrolamentos: "+str(multiplicador)) #alterado Portaria 06/2021 
        if medCautelar=="S":
            multiplicador+=0.5  
            multPar.append("Med Cautelar: 0.5")      
        multiplicador = multiplicador + 0.1*rffps + (0.05 if inaptidoes>0 else 0) + (0.1 if baixas>0 else 0) + (0.35 if excSimples>0 else 0)   
        multPar.append("RFFPs/Improbidade: "+str(0.1*rffps))
        multPar.append(("Inaptidões: 0.05" if inaptidoes>0 else "Inaptidões: 0")) 
        multPar.append(("Baixas: 0.1" if baixas>0 else "Baixas: 0"))
        multPar.append(("Exclusão Simples: 0.35" if excSimples>0 else "Exclusão Simples: 0"))
        if acompanhamento=="S": #acompanhamento Comac
            if len(ni)==18: #PJ
                multiplicador+=0.4 
                multPar.append("Acompanhamento: 0.4")
            else: #PF
                multiplicador+=1.4 #0.3 #alterado Portaria 06/2021
                multPar.append("Acompanhamento: 1.4")      
        if 1<=sujPassivos<=2:
            multiplicador+=0.15
            multPar.append("Suj Passivos: 0.15")
        elif 2<sujPassivos<=10:
            multiplicador+=0.25
            multPar.append("Suj Passivos: 0.25")
        elif sujPassivos>10:
            multiplicador+=0.35    
            multPar.append("Suj Passivos: 0.35")         
        if 0<digVincs<=5:
            multiplicador+=0.1
            multPar.append("Dilig Vinculadas: 0.1")
        elif 5<digVincs<=20:
            multiplicador+=0.2
            multPar.append("Dilig Vinculadas: 0.2")
        elif 20<digVincs<=60:
            multiplicador+=0.3
            multPar.append("Dilig Vinculadas: 0.3")
        elif digVincs>60:
            multiplicador+=0.4
            multPar.append("Dilig Vinculadas: 0.4")
        if situacao11=="S" and operacao40111=="S" and len(ni)==11: #fiscalização PF, operação 40111 e usuário marcou, então pode ter o acréscimo
            multiplicador+=0.2
            multPar.append("Situação 11: 0.2")
        if qtdeOperacoes==2:
            multiplicador+=0.05
            multPar.append("Qtde Operações: 0.05")
        elif qtdeOperacoes>2:
            multiplicador+=0.1 
            multPar.append("Qtde Operações: 0.1")     
        if interposicao=="S":
            multiplicador+=0.3
            multPar.append("Interposição: 0.3")
        if qtdeAnos==2:
            multiplicador+=0.1
            multPar.append("Qtde Anos: 0.1")
        elif qtdeAnos>2:
            multiplicador+=0.2
            multPar.append("Qtde Anos: 0.2")
        if situacao15=="S":
            multiplicador+=0.3
            multPar.append("Situação 15: 0.3")
        if 2141 in listaTributos or 2096 in listaTributos: #situação 16 só se aplica a tributos previdenciários
            multPrev1 = 0
            multPrev2 = 0
            if 25<=estabPrev1<100:
                multPrev1=0.1
            elif 100<=estabPrev1<250:
                multPrev1=0.2
            elif estabPrev1>=250:
                multPrev1=0.3
            if 10<=estabPrev2<20:
                multPrev2=0.1
            elif 20<=estabPrev2<50:
                multPrev2=0.2
            elif estabPrev2>=50:
                multPrev2=0.3 
            multPar.append("Estabelec Previd: "+str(multPrev1))   
            multPar.append("CEI/CAEPF/Obra: "+str(multPrev2))   
            multiplicador+=max(multPrev1, multPrev2)           
        if 2096 in listaTributos: #situação 17 só se aplica a fiscalização da contribuição do segurado
            multPrev = 0
            if 250<=segurados<500:
                multPrev=0.05
            elif 500<=segurados<1000:
                multPrev=0.1
            elif segurados>=1000:
                multPrev=0.2              
            multiplicador+=multPrev
            multPar.append("Segurados: "+str(multPrev))   
        if 2141 in listaTributos or 2096 in listaTributos: #situações 18  e a 19 só se aplicam a tributos previdenciários
            multPrev1 = 0
            multPrev2 = 0                
            if 10<=prestadores<25:
                multPrev1=0.1
            elif 25<=prestadores<40:
                multPrev1=0.2
            elif prestadores>=40:
                multPrev1=0.3  
            if 5<=tomadores<15:
                multPrev2=0.1
            elif 15<=tomadores<30:
                multPrev2=0.2
            elif tomadores>=30:
                multPrev2=0.3 
            multPar.append("Prestadores: "+str(multPrev1))   
            multPar.append("Tomadores: "+str(multPrev2))   
            multiplicador = multiplicador+multPrev1+multPrev2
        if 3880 in listaTributos or 6121 in listaTributos or 1011 in listaTributos: #situação 20 só se aplica a PIS, Cofins e IPI
            multPer = 0
            if 9<=qtdePER<17:
                multPer = 0.1
            elif 17<=qtdePER<25:
                multPer = 0.15
            elif qtdePER>=25:
                multPer = 0.2
            multPar.append("Qtde PER: "+str(multPer))   
            multiplicador+=multPer       
        if lancMuldi=="S":
            multiplicador+=0.05
            multPar.append("Lanc Muldi: 0.05")   
        if compensacao=="S":
            multiplicador+=0.3
            multPar.append("Compensação Não CPRB: 0.3")   
        if creditoExt=="S" and (3880 in listaTributos or 6121 in listaTributos or 1011 in listaTributos): #situação 23 só se aplica a PIS, Cofins e IPI
            multiplicador+=0.15 
            multPar.append("Crédito Extemp: 0.15")   
        #print("Multiplicador", multiplicador)  
        totalPontos = str(int(pontos+baseMultiplicador*multiplicador)).rjust(4,"0")
        #print("Total Pontos "+totalPontos)
        return "24E"+encerrado+totalPontos+parametros  
    else:
        if pediuParametros:
            return "24E"+encerrado+porte+acompanhamento+str(qtdeOperacoes).rjust(3,"0")+str(qtdeAnos)+str(tributoPrincipal).rjust(4,"0")+str(opPrincipal).rjust(5, "0")
        else: #não pediu parâmetros internos nem informou os do usuário, então só enviamos 0000 e o status do encerramento
            return "24P"+encerrado+"0000"
    return             


def trataMsgRecebida(msgRecebida, c, addr): #c é o socket estabelecido com o cliente que será utilizado para a resposta
    global chavesCripto, ambiente, CPF1, CPF2, CPF3, SENHADCCS
    tamChave = 6 #tamanho da chave do ContÁgil (chave de registro) 
    tamMsg = 100
    tamNome = 100
    try: #tentamos obter mensagens descriptografadas - solicitação de chave criptográfica pública ou solicitação de envio de chave/senha para o e-mail
        msgOrigem = msgRecebida.decode("utf-8")     
        if len(msgOrigem)==13:
            if msgOrigem[:2]=="00": #está fazendo uma requisição de chave pública (msg sem criptografia)
                cpf = msgOrigem[2:]
                if not validaCPF(cpf):
                    resposta = "97CPF INVÁLIDO"
                    enviaResposta(resposta, c)        
                    return                
                if not estaoChavesValidas(addr): #meio difícil de ocorrer aqui, pq tem lá no servidor, mas ...
                    geraChaves(addr) 
                versaoScript = "12a" #versão mínima do Script (X.XX, sem o ponto) - só informa na mensagem abaixo (não restringe nas requisições)
                enviaRespostaSemFechar("0000"+chavesCripto[segmentoIP(addr)][2].strftime("%d/%m/%Y %H:%M:%S")+versaoScript, c) #envia a validade da chave e a versão mínima exigida do Script
                try:
                    msg = c.recv(1024).decode("utf-8") #só aguarda um 00
                    if msg=="00":
                        #print(len(chavesCripto[segmentoIP(addr)][0].publickey().export_key()))
                        c.sendall(chavesCripto[segmentoIP(addr)][0].publickey().export_key()) #envia a chave pública (tamanho = 450)
                except:
                    logging.info("Não enviou o flag para receber a chave "+msgOrigem[2:])
                c.close()
                return
            elif msgOrigem[:2]=="26": #está fazendo uma requisição de envio de chave/senha para o e-mail institucional vinculado ao CPF
                cpf = msgOrigem[2:]
                if not validaCPF(cpf):
                    resposta = "97CPF INVÁLIDO"
                    enviaResposta(resposta, c)        
                    return                      
                conn = conecta()
                if not conn:
                    resposta = "97ERRO NA CONEXÃO COM O BANCO DE DADOS" #erro de conexão ou de BD
                    enviaResposta(resposta, c) 
                    return   
                cursor = conn.cursor(buffered=True)                                   
                comando = "Select Codigo, email, DataEnvio from Usuarios Where CPF=%s"
                cursor.execute(comando, (cpf,))
                row = cursor.fetchone()
                if not row or len(row)==0:
                    resposta = "97CPF NÃO CONSTA DA BASE DE DADOS DO SERVIÇO"
                    enviaResposta(resposta, c) 
                    return  
                codigoReg = row[0]
                email = row[1]
                dataEnvio = row[2]  
                if email==None:
                    resposta = "26USUÁRIO NÃO TEM EMAIL CADASTRADO NA BASE - CONTACTE botespontaneidade@rfb.gov.br" 
                    enviaResposta(resposta, c) 
                    return 
                if not '@RFB.GOV.BR' in email.upper():
                    resposta = "26EMAIL DO USUÁRIO CADASTRADO NA BASE NÁO É INSTITUCIONAL - CONTACTE botespontaneidade@rfb.gov.br"
                    enviaResposta(resposta, c) 
                    return  
                if dataEnvio!=None:     
                    if dataEnvio.date()>=datetime.now().date():
                        resposta = "26SOMENTE É PERMITIDA UMA REQUISIÇÃO DESTA POR DIA"
                        enviaResposta(resposta, c) 
                        return 
                chave = randint(100000, 1000000) #a chave é um número inteiro de seis dígitos
                message = "Prezado(a),\n\nSua chave SIGILOSA de registro no Bot Espontaneidade (Telegram) é "+str(chave)+"\n\nEsta chave é utilizada também para acesso via ContÁgil no Script AlertasFiscalização e tem validade de 30 dias.\n\nAtenciosamente,\n\nDisav/Cofis\n\nAmbiente: "+ambiente 
                assunto = "Chave de Registro - Bot Espontaneidade"
                sucesso = enviaEmail(email, message, assunto)
                if sucesso==3:
                    comando = "Update Usuarios Set Chave=%s, ValidadeChave=%s, Tentativas=%s, DataEnvio=%s Where Codigo=%s"
                    validade = datetime.today().date()+timedelta(days=30) #a chave tem validade de 30 dias
                    try:
                        cursor.execute(comando, (chave, validade, 0, datetime.today().date(), codigoReg))
                        conn.commit()
                        resposta = "CHAVE FOI ENVIADA PARA O E-MAIL INSTITUCIONAL E ATUALIZADA NA BASE DE DADOS."
                    except:
                        conn.rollback()    
                        resposta = "ERRO AO ATUALIZAR A TABELA DE USUÁRIOS - A CHAVE ENVIADA PARA O E-MAIL NÃO É VALIDA"
                else:
                    resposta = "ERRO AO TENTAR ENVIAR O E-MAIL"
                enviaResposta("26"+resposta, c) 
                return                     
    except:
        pass #não conseguiu decodificar a msgRecebida

    if not estaoChavesValidas(addr):
        resposta = "99REQUISIÇÃO RECUSADA - CHAVE CRIPTOGRÁFICA VENCIDA" #código de erro na mensagem recebida
        enviaResposta(resposta, c)
        return        

    msgRecebida = descriptografa(msgRecebida, addr, c)
    if msgRecebida=="000000000A": #houve falha na descriptografia
        resposta = "99REQUISIÇÃO RECUSADA - CRIPTOGRAFIA INVÁLIDA - REINICIE O SCRIPT" #código de erro na mensagem recebida
        enviaResposta(resposta, c)
        return   
    c.settimeout(10)
    #todas as mensagens tem, no mínimo, um código, um cpf para acesso e uma chave deste - total: 19 caracteres (chave de 6 dígitos)
    if len(msgRecebida)<(13+tamChave):
        resposta = "99REQUISIÇÃO INVÁLIDA (A)" #código de erro na mensagem recebida
        enviaResposta(resposta, c)
        return 
    
    codigoStr = msgRecebida[:2]
    cpf = msgRecebida[2:13]  
    chaveContagil = msgRecebida[13:(13+tamChave)]      

    logging.info(codigoStr+" - "+cpf)
    #logging.info(chaveContagil)     
   
    if not codigoStr.isdigit():
        resposta = "99REQUISIÇÃO INVÁLIDA (B)"
        enviaResposta(resposta, c)       
        return  
    try:
        codigo = int(codigoStr)             
    except:
        resposta = "99REQUISIÇÃO INVÁLIDA (B1)"
        enviaResposta(resposta, c)       
        return         
    
    if cpf=="12345678909": #este CPF flag não vem por aqui - ele é esperado dentro de alguns lugares neste procedimento para mandar mais informações da requisição
        resposta = "97CPF INVÁLIDO PARA ESTA REQUISIÇÃO"
        enviaResposta(resposta, c)        
        return
    
    if not validaCPF(cpf):
        resposta = "97CPF INVÁLIDO"
        enviaResposta(resposta, c)        
        return  

        
    if codigo<1 or (codigo>40 and codigo!=60): #número de requisições válidas
        resposta = "99CÓD DA REQUISIÇÃO É INVÁLIDO (C)" 
        enviaResposta(resposta, c)          
        return     
             
    if not chaveContagil.isdigit():
        resposta = "90CHAVE DE ACESSO VIA CONTAGIL NÃO É NUMÉRICA"
        enviaResposta(resposta, c)        
        return     
    #dbpath = "C:\\Users\\marco\\Downloads\\"
    #db = "BotTelegramCofisDisaf.accdb"
    #driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    #conn = pyodbc.connect("DRIVER={};DBQ={}".format(driver, dbpath+db))  #estabelece conexão com o BD
    conn = conecta()
    if not conn:
        resposta = "97ERRO NA CONEXÃO COM O BANCO DE DADOS" #erro de conexão ou de BD
        enviaResposta(resposta, c) 
        return            
    cursor = conn.cursor(buffered=True)    

    if codigo!=18 and ambiente!="TESTE": #fazemos o log no ambiente de produção, exceto para envio de entrada do diário da fiscalização 
                                         #e solicitação de chave pública (acima - cód = 00)
        comando = "Insert Into Log (IP, Requisicao, Mensagem, Data) Values (%s, %s, %s, %s)"
        try:
            cursor.execute(comando, (c.getpeername()[0], codigo, msgRecebida[2:], datetime.now()))
            conn.commit()
        except:
            logging.info("Falhou o log - IP: "+c.getpeername()[0]+"; Msg: "+msgRecebida)
            conn.rollback()

    if codigo==1: #status do usuário 
        comando = "Select Codigo, CPF, Adesao, Saida, d1, d2, d3, email, Chave, ValidadeChave, Tentativas, Orgao from Usuarios Where CPF=%s"
        cursor.execute(comando, (cpf,))
        row = cursor.fetchone() 
        if row==None or len(row)==0:
            resposta = "0104" #01 - status; 04 - não consta na base (não foram carregados)
            enviaResposta(resposta, c) 
            conn.close()
            return            
        ativo = False
        registrado = False
        if row[3]==None and row[2]!=None: #tem um registro ativo
            ativo = True
        if row[8]!=None: #usuário tem chave cadastrada (está registrado)
            registrado = True            
            tentativas = row[10]
            if tentativas==None:
                tentativas = 0
            chaveBD = row[8]
            if tentativas>=3:
                result = "4"
            elif chaveBD==int(chaveContagil):
                if row[9]==None:
                    result = "2"
                else:  
                    logging.info(row[9].date())                        
                    if datetime.today().date()<=row[9].date():
                        result = "1"+row[9].strftime("%d/%m/%Y") #chave dentro da validade
                        zeraTentativas(row[0], conn)
                    else:
                        result = "2" #chave fora da validade
            else: #chave digitada não confere 
                result = "3"
                atualizaTentativas(row[0], tentativas, conn)
        orgao = row[11]
        tipoOrgaoUsuario = buscaTipoOrgao(orgao, cursor)        
        if ativo:
            resposta = "0101"+tipoOrgaoUsuario+result #01 - status; 01 - ativo
        elif registrado:
            resposta = "0102"+tipoOrgaoUsuario+result #01 - status; 02 - inativo no Bot (não tem senha ou saiu)
        else:
            resposta - "0103"+tipoOrgaoUsuario #01 - status; 03 - não registrado (nem tem senha)
        enviaResposta(resposta, c) 
        conn.close()
        return    
    
    
    #validamos a chave do contágil ligada àquele CPF (registro ativo) - serviços de 2 em diante
    comando = "Select Codigo, Chave, ValidadeChave, Tentativas, email, d1, d2, d3, Orgao, Adesao, Saida from Usuarios Where CPF=%s"            
    cursor.execute(comando, (cpf,))
    row = cursor.fetchone()   
    if not row or len(row)==0: #o usuário está inativo
        resposta = "90USUÁRIO NÃO ENCONTRADO NA BASE DE DADOS"
        enviaResposta(resposta, c)  
        conn.close()
        return
    orgaoUsuario =  row[8]
    tipoOrgaoUsuario = buscaTipoOrgao(orgaoUsuario, cursor) 
    if (row[9]==None or row[10]!=None) and (tipoOrgaoUsuario=="L" or not codigo in [13, 24, 25, 28, 29]): #adesão nula ou inatividade
                                                                                                          #só permitimos acesso para usuários nacionais e regionais ou para as 
                                                                                                          #requisições listadas
        if row[9]==None:
            resposta = "90USUÁRIO NÃO SE REGISTROU NO BOT TELEGRAM OU OPÇÃO NÃO DISPONÍVEL PARA O TIPO DE USUÁRIO"
        else:
            resposta = "90USUÁRIO ESTÁ INATIVO NO BOT TELEGRAM OU OPÇÃO NÃO DISPONÍVEL PARA O TIPO DE USUÁRIO"
        enviaResposta(resposta, c)  
        conn.close()
        return           
    tentativas = row[3]
    if tentativas==None:
        tentativas = 0         
    if row[1]==None:
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL NÃO FOI GERADA"       
        enviaResposta(resposta, c) 
        conn.close()
        return        
    if row[1]!=int(chaveContagil):
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL É INVÁLIDA OU INCORRETA"
        atualizaTentativas(row[0], tentativas, conn)        
        enviaResposta(resposta, c)
        conn.close()
        return
    if row[2]==None:
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL SEM VALIDADE - GERE OUTRA"
        enviaResposta(resposta, c) 
        conn.close()
        return        
    if datetime.today().date()>row[2].date():
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL ESTÁ EXPIRADA - GERE OUTRA"
        enviaResposta(resposta, c)  
        conn.close()
        return    
    if tentativas>=3:
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL ESTÁ EXPIRADA - TENTATIVAS EXCEDIDAS - GERE OUTRA"
        enviaResposta(resposta, c) 
        conn.close()
        return               
    
    zeraTentativas(row[0], conn) #como a chave está correta, zera o nº de tentativas
        
    #para todas as funções abaixo, temos que verificar se o cpf está cadastrado e ativo <- JÁ FOI FEITO ACIMA
    #comando = "Select Codigo, CPF, Adesao, Saida, d1, d2, d3, email, Chave, ValidadeChave, Tentativas from Usuarios Where Saida Is Null and Adesao Is Not Null and CPF=%s"        
    #cursor.execute(comando, (cpf,))
    #row = cursor.fetchone()
    #if not row:
    #    resposta = "99CPF NÃO ENCONTRADO OU INATIVO NO SERVIÇO"
    #    enviaResposta(resposta, c)
    #    conn.close()
    #    return 

    #if len(row)==0:
    #    resposta = "99CPF NÃO ENCONTRADO OU INATIVO NO SERVIÇO"
    #    enviaResposta(resposta, c)
    #    conn.close()
    #    return           

    if codigo==22: #troca de senha
        if len(msgRecebida)!=(13+2*tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (22A)"
            enviaResposta(resposta, c)  
            conn.close()
            return 
        novaChave = msgRecebida[-6:]
        if not novaChave.isdigit():
            resposta = "22NCHAVE DEVE SER NUMÉRICA (A)"
            enviaResposta(resposta, c)  
            conn.close()
            return
        if novaChave==chaveContagil:
            resposta = "22NVALIDAÇÃO FEITA PELO SCRIPT FOI BURLADA"
            enviaResposta(resposta, c)  
            conn.close()
            return 
        try:
            chaveNum = int(novaChave)
        except:
            resposta = "22NCHAVE DEVE SER NUMÉRICA (B)"
            enviaResposta(resposta, c)  
            conn.close()
            return
        if chaveNum<100000 or chaveNum>999999:
            resposta = "22NCHAVE DEVE CONTER 6 ALGARISMOS, SENDO O PRIMEIRO DIFERENTE DE ZERO"
            enviaResposta(resposta, c)  
            conn.close()
            return 
        if (novaChave in '12345678909876543210'):  
            resposta = "22NCHAVE NÃO PODE SER UMA SEQUÊNCIA DE ALGARISMOS"
            enviaResposta(resposta, c)  
            conn.close()
            return   
        if (novaChave in ['111111', '222222', '333333', '444444', '555555', '666666', '777777', '888888', '999999']):  
            resposta = "22NCHAVE NÃO PODE SER COMPOSTA DOS MESMOS ALGARISMOS"
            enviaResposta(resposta, c)  
            conn.close()
            return                                
        comando = "Update Usuarios Set Chave=%s, ValidadeChave=%s Where CPF=%s"
        try:
            validade = datetime.now().date()+timedelta(days=30)
            cursor.execute(comando, (chaveNum, validade, cpf))
            resposta = "22STROCA DE SENHA EFETUADA - CHAVE VÁLIDA ATÉ "+validade.strftime("%d/%m/%Y")
            conn.commit()
        except:
            resposta = "22NERRO AO ATUALIZAR TABELA"   
            conn.rollback()       
        enviaResposta(resposta, c)  
        conn.close()
        return 


    #obtemos a chave do fiscal para ser utilizada em todas as requisições  que não sejam a troca de senha, nem status - acima  
    comando = "Select Fiscais.Codigo From Fiscais Where Fiscais.CPF=%s"          
    cursor.execute(comando, (cpf,))
    rowFiscal = cursor.fetchone()
    if (not rowFiscal or len(rowFiscal)==0) and not codigo in [13, 24, 25, 28, 29]: #estes códigos podem ser utilizados por usuários Cofis ou Difis
        resposta = "97CPF NÃO FOI LOCALIZADO NA TABELA DE FISCAIS/SUPERVISORES"
        enviaResposta(resposta, c)  
        conn.close()
        return 
    elif rowFiscal!=None:
        chaveFiscal = rowFiscal[0] #<---
    else:
        chaveFiscal = 0

    if codigo in [2, 3, 4, 5, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 27, 28, 30, 31, 33, 34, 35, 36, 38, 40]: 
    #verificações COMUNS relativas ao TDPF - TDPF existe, em andamento (p/ alguns), cpf está alocado nele
        if len(msgRecebida)<(29+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (D)"
            enviaResposta(resposta, c)  
            conn.close()
            return                   
        tdpf = msgRecebida[(tamChave+13):(tamChave+29)] #obtemos o TDPF que será utilizado em todas as requisições acima elencadas
        if not tdpf.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - TDPF DEVE SER NUMÉRICO"
            enviaResposta(resposta, c)  
            conn.close()
            return            
        comando = "Select Codigo, Encerramento, Nome, Emissao, Vencimento, Grupo, NI, DCC, Porte, Acompanhamento, TrimestrePrevisto from TDPFS Where Numero=%s"        
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        if row:
            chaveTdpf = row[0] #<-- chave do TDPF que será utilizado nas requisições aqui tratadas
            encerramento = row[1]
            emissao = row[3] #<-- data de emissão fica disponível
            nome = row[2] #<-- nome do fiscalizado tb
            if nome==None or nome=="":
                nome = "NOME INDISPONÍVEL"
            nome = nome[:tamNome].ljust(tamNome)
            if encerramento!=None and not codigo in [4, 15, 19, 21, 23, 24, 27, 28, 30, 31, 34]: #nestes códigos podemos listar ciências, atividades, entradas do diário, informar DCC,
                                                                                 #incluir/listar pontuação de TDPFs encerrados, incluir trimestre de encerramento previsto,
                                                                                 # mostrar fiscais alocados ou informar se é supervisor do tdpf (mesmo encerrado), dados do TDPF tb
                                                                                 #listar prorrogações
                msg = "TDPF encerrado"
                msg = msg.ljust(tamMsg)
                resposta = codigoStr+(("N"+msg+nome) if (2<=codigo<=5) else msg) #o código 27 estava aqui antes de colocar na lista acima
                if codigo in [33, 35, 36, 38, 40]:
                    resposta = codigoStr+"E"
                enviaResposta(resposta, c) 
                conn.close()
                return               
        else: 
            msg = "TDPF NÃO foi localizado ou foi encerrado há muito tempo e não colocado na base deste serviço"
            msg = msg.ljust(tamMsg)          
            if codigo in [30, 31, 33, 35, 36, 38, 40]:
                resposta = codigoStr+"I"
            else:
                resposta = codigoStr+(("I"+msg) if (2<=codigo<=5 or codigo in [23, 24, 27, 28]) else msg)
            enviaResposta(resposta, c) 
            conn.close()
            return  
        rowTdpf = row #para utilizarmos na requisição 31            
        comando = "Select Alocacoes.Desalocacao from Alocacoes Where Alocacoes.Fiscal=%s and Alocacoes.TDPF=%s"
        cursor.execute(comando, (chaveFiscal, chaveTdpf))
        row = cursor.fetchone()    
        bSupervisor = False 
        if codigo in [4, 15, 21, 23, 24, 27, 28, 31, 34, 35, 36]: #supervisor pode relacionar ciências (4), atividades (15) de TDPF, incluir ou lista pontuação (23, 24),
                                                  #incluir DCC (21), informar trimestre da meta (27) ou listar fiscais alocados (28) ou dados do TDPF (31) e apagar/assinar prorrogação
                                                  #listar prorrogações
            if tipoOrgaoUsuario=="N" and codigo in [24, 28, 31]: #nestes códigos, usuário Cofis pode fazer consulta (#para os fins destas requisições, é supervisor)
                bSupervisor = True
            elif tipoOrgaoUsuario=="R" and codigo in [24, 28, 31]: #nestes códigos, usuário Difis pode fazer consulta (#para os fins destas requisições, é supervisor)
                comando = "Select TDPFS.Numero from TDPFS Where TDPFS.Codigo=%s and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s)"
                cursor.execute(comando, (chaveTdpf, orgaoUsuario))
                rowSuperv = cursor.fetchone()
                if rowSuperv:
                    bSupervisor = True
            if not bSupervisor:
                bSupervisor, _ = verificaSupervisao(conn, cpf, tdpf)   
        if codigo==30: #a requisição é uma pergunta se o cpf é do supervisor do tdpf ou está alocado nele    
            bSupervisor, _ = verificaSupervisao(conn, cpf, tdpf)       
            if row!=None: #fiscal esteve ou está alocado do tdpf
                if row[0]==None: #fiscal está ainda alocado ao tdpf
                    alocado = "S"
                else:
                    alocado = "N"                    
            else:
                alocado = "N"
            if bSupervisor:
                supervisor = "S"
            else:
                supervisor = "N"           
            resposta = "30"+alocado+supervisor
            enviaResposta(resposta, c)   
            conn.close()
            return  
        if not bSupervisor and codigo==27: #só supervisor pode incluir trimestre da meta 
            msg = "Usuário NÃO é supervisor do TDPF"
            resposta = "27"+msg.ljust(tamMsg)            
            enviaResposta(resposta, c)   
            conn.close()
            return              
        achou = False
        if row and not bSupervisor:
            achou = True
            if row[0]!=None and not codigo in [28, 31]: #fiscal desalocado pode consultar TDPFs em que participou (28 - lista fiscais alocados no TDPF; 31 - dados do TDPF; no script não terá como num primeiro momento)
                msg = "CPF NÃO está mais alocado ao TDPF ou não é supervisor, em requisições em que isso seria relevante"
                msg = msg.ljust(tamMsg)                
                resposta = codigoStr+(("N"+msg+nome) if (2<=codigo<=5 or codigo==23) else msg)
                enviaResposta(resposta, c)  
                conn.close()
                return                         
        if not achou and not bSupervisor:
            msg = "CPF NÃO está alocado ao TDPF ou não é supervisor, em requisições em que isso seria relevante"
            msg = msg.ljust(tamMsg)   
            if codigo in [31, 33, 34, 35, 36, 38, 40]:
                resposta = codigoStr+"N"
            else:
                resposta = codigoStr+(("N"+msg+nome) if (2<=codigo<=5 or codigo in [23, 24]) else msg)     
            enviaResposta(resposta, c)   
            conn.close()
            return    

    if codigo==31: #pediu as informações do registro do tdpf na tabela TDPFS - todas as verificações foram feitas acima e o resultado está em rowTdpf
        grupo = rowTdpf[5]
        if grupo==None:
            grupo = ""
        grupo = grupo.ljust(25)
        emissao = rowTdpf[3]
        if emissao==None:
            emissao="00/00/0000"
        else:
            emissao = emissao.strftime("%d/%m/%Y")
        encerramento = rowTdpf[1]
        if encerramento==None:
            encerramento = "00/00/0000"
        else:
            encerramento = encerramento.strftime("%d/%m/%Y") 
        nome = rowTdpf[2]
        if nome==None:
            nome = ""
        nome = nome.ljust(150)
        niFiscalizado = rowTdpf[6]
        if niFiscalizado==None:
            niFiscalizado = ""
        niFiscalizado = niFiscalizado.ljust(18)  
        vencimento = rowTdpf[4]
        if vencimento==None:
            vencimento = "00/00/0000"
        else:
            vencimento = vencimento.strftime("%d/%m/%Y")
        dcc = rowTdpf[7]
        if dcc==None:
            dcc = ""
        dcc = dcc.ljust(17)
        porte = rowTdpf[8]
        if porte==None:
            porte = ""
        porte = porte.ljust(3)
        acompanhamento = rowTdpf[9]
        if acompanhamento==None:
            acompanhamento = " "
        trimestrePrevisto = rowTdpf[10]
        if trimestrePrevisto==None:
            trimestrePrevisto = "      "
        #procuramos o supervisor atual do grupo do TDPF
        comando = "Select Fiscais.CPF, Fiscais.Nome from Fiscais, Supervisores Where Supervisores.Fiscal=Fiscais.Codigo and Supervisores.Fim Is Null and Supervisores.Equipe=%s"
        cursor.execute(comando, (grupo.strip(),))
        row = cursor.fetchone()
        if not row:
            cpfSuperv = "".ljust(11)
            nomeSuperv = "".ljust(100)
        else:
            cpfSuperv = row[0]
            nomeSuperv = row[1][:100].ljust(100)
        resposta = "31S"+ grupo+emissao+encerramento+nome+niFiscalizado+vencimento+dcc+vencimento+porte+acompanhamento+trimestrePrevisto+cpfSuperv+nomeSuperv
        enviaResposta(resposta, c)   
        conn.close()
        return
           
    if codigo==2: #informa data de ciência relativa a TDPF 
        try:   #deve enviar imediatamente a descrição do documento que efetivou a ciência (sem criptografia)
            mensagemRec = c.recv(512) #.decode('utf-8') #chegou a requisicao
        except:
            c.close()
            logging.info("Erro de time out 2 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
            conn.close()
            return         
        if len(msgRecebida)!=(49+tamChave): #inclui o tdpf, a data da intimação e a data de seu vencimento
            resposta = "99REQUISIÇÃO INVÁLIDA (2A)"
            enviaResposta(resposta, c) 
            conn.close()
            return      
        
        data = msgRecebida[-20:-10] 
        if not isDate(data):
            msg = "Data de ciência inválida"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome 
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            dataObj = datetime.strptime(data, '%d/%m/%Y')
        except:
            msg = "Data de ciência inválida (2)"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome 
            enviaResposta(resposta, c) 
            conn.close()
            return            
        if datetime.today().date()<dataObj.date(): #data não pode ser futura
            msg = "Data de ciência não pode ser futura"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome 
            enviaResposta(resposta, c)  
            conn.close()
            return
        if dataObj.date()<emissao.date(): #ciência não pode ser inferior à data de emissão (obtida nas verificacões gerais)
            msg = "Data de ciência não pode ser inferior à de emissão ("+emissao.strftime("%d/%m/%Y")+")"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome 
            enviaResposta(resposta, c)  
            conn.close()
            return   
        # estou permitindo informar data de ciência anterior à última por questões de registro - manter a crítica depois de um certo tempo                     
        #comando = "Select Data from Ciencias Where TDPF=%s and Data>=%s Order by Data DESC"
        #cursor.execute(comando, (chaveTdpf, dataObj.date()))
        #row = cursor.fetchone()
        #if row:
        #    msg = "Data de ciência informada DEVE ser posterior à ultima informada para o TDPF ("+row[0].strftime('%d/%m/%Y')+")"
        #    msg = msg.ljust(tamMsg)             
        #    resposta = "02N"+msg+nome
        #    enviaResposta(resposta, c)  
        #    conn.close()
        #    return  

        vencimento = msgRecebida[-10:] 
        if vencimento=="00/00/0000":
            vencimentoObj = None
        else:
            if not isDate(vencimento):
                msg = "Data de vencimento da intimação inválida"
                msg = msg.ljust(tamMsg)             
                resposta = "02N"+msg+nome 
                enviaResposta(resposta, c) 
                conn.close()
                return
            try:
                vencimentoObj = datetime.strptime(vencimento, '%d/%m/%Y').date()
            except:
                msg = "Data de vencimento da intimação inválida (2)"
                msg = msg.ljust(tamMsg)             
                resposta = "02N"+msg+nome 
                enviaResposta(resposta, c) 
                conn.close()
                return            
            if vencimentoObj<=dataObj.date(): #vencimento da intimação não pode ser inferior à data de ciência
                msg = "Data de vencimento da intimação deve ser posterior à de ciência."
                msg = msg.ljust(tamMsg)             
                resposta = "02N"+msg+nome 
                enviaResposta(resposta, c)  
                conn.close()
                return 

        requisicao = descriptografa(mensagemRec, addr, c) 
        if requisicao=="000000000A": #não foi possível descriptografar
            resposta = "99REQUISIÇÃO INVÁLIDA - NÃO FOI POSSÍVEL DESCRIPTOGRAFAR O DOCUMENTO (2A1)"
            enviaResposta(resposta, c) 
            conn.close()
            return            
        codReq = ""
        if len(requisicao)>=2:
            codReq = requisicao[:2]
        if len(requisicao)!=72 or codReq!="02":
            msg = "Documento não foi informado ou requisição inválida."
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return
        documento = requisicao[2:].strip().upper()    
        if len(documento)<3:
            msg = "Documento não foi informado ou tem menos de 3 caracteres."
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return   
        if len(documento)>70: #isso aqui é só uma garantia extrema
            msg = "Documento tem mais de 70 caracteres."
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return                                          
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)
        try:
            comando = "Insert into Ciencias (TDPF, Data, Documento, Vencimento) Values (%s, %s, %s, %s)"
            cursor.execute(comando, (chaveTdpf, dataObj.date(), documento, vencimentoObj))
            msg = "Ciência registrada para o TDPF"
            if tdpfMonitorado and monitoramentoAtivo==False: #monitoramento do tdpf estava desativado - ativa
                msg = "Monitoramento deste TDPF foi reativado e a ciência foi registrada."
                comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                cursor.execute(comando, (chave,))                         
            elif not tdpfMonitorado: #tdpf não estava sendo monitorado - inclui ele
                comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date()))
            conn.commit()
            msg = msg.ljust(tamMsg) 
            resposta = "02S"+msg+nome          
        except:
            conn.rollback()
            msg = "Erro ao atualizar as tabelas."  
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
        enviaResposta(resposta, c)  
        conn.close()
        return            
        
    if codigo==3: #anula ciência relativa ao TDPF
        comando = "Select Codigo, TDPF, Data from Ciencias Where TDPF=%s Order by Data DESC"
        cursor.execute(comando, (chaveTdpf,))
        rows = cursor.fetchall()
        if rows==None or len(rows)==0:
            msg  = "Não há data de ciência informada para o TDPF" #Não havia data de ciência para o TDPF
            msg = msg.ljust(tamMsg)             
            resposta = "03N"+msg+nome
            enviaResposta(resposta, c)     
            conn.close()
            return   
        #print(rows)         
        if len(rows)==1:
            dataAnt = "Nenhuma Data Vigente" #não haverá data anterior
        else: #2 ou mais linhas
            dataAnt = "Data agora em vigor: "+rows[1][2].strftime('%d/%m/%Y')        
        chave = rows[0][0]    
        try:
            comando = "Delete from Ciencias Where Codigo=%s"
            cursor.execute(comando, (chave,))
            conn.commit() 
            msg = dataAnt.ljust(tamMsg)            
            resposta = "03S"+msg+nome
            enviaResposta(resposta, c)   
            conn.close()
            return            
        except:
            conn.rollback()
            msg = "Erro na atualização das tabelas. Tente novamente mais tarde."        
            msg = msg.ljust(tamMsg)             
            resposta = "03N"+msg+nome
            enviaResposta(resposta, c)      
            conn.close()
            return            
        
    if codigo==4: #relaciona ciências de um tdpf
        comando = "Select Data, Documento, Vencimento from Ciencias Where TDPF=%s Order by Data"
        cursor.execute(comando, (chaveTdpf,))
        rows = cursor.fetchall()
        if len(rows)==0:
            msg = "Não há nenhuma ciência registrada para o TDPF."
            msg = msg.ljust(tamMsg)
            resposta = "04N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return            
        nn = len(rows)
        if nn<10:
            nn = "0"+str(nn)
        elif nn>99:
            nn = "99"
        else:
            nn = str(nn)
        datas = ""
        i = 0
        for row in rows:
            documento = row[1]
            if not documento:
                documento = ""
            documento = documento.ljust(70)
            vencimento = dataTexto(row[2])
            datas = datas + row[0].strftime('%d/%m/%Y')+documento+vencimento
            i+=1
            if i>=30: #limite de 30 datas/documentos
                break
        resposta = "04S"+nn+nome+datas
        enviaResposta(resposta, c)  
        conn.close()
        return        
    
    if codigo==5: #finaliza alertas de um tdpf
        dataAtual = datetime.today().date()
        comando = "Update CadastroTDPFs Set Fim=%s Where TDPF=%s and Fiscal=%s"
        try:
            cursor.execute(comando, (dataAtual, chaveTdpf, chaveFiscal))
            conn.commit()
            msg = "TDPF não será mais objeto de alerta"
            msg = msg.ljust(tamMsg)
            resposta = "05S"+msg+nome
        except:
            conn.rollback()
            msg = "Erro na atualização das tabelas. Tente novamente mais tarde."        
            msg = msg.ljust(tamMsg)             
            resposta = "05N"+msg+nome            
        enviaResposta(resposta, c)  
        conn.close()
        return 
        
    if codigo==6: #mostra lista de tdpfs ativos e últimas ciências 
        if len(msgRecebida)>(13+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (6A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        comando = """Select TDPFS.Codigo, TDPFS.Numero, TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao
                     from CadastroTDPFs, Alocacoes, TDPFS 
                     Where CadastroTDPFs.Fiscal=%s and CadastroTDPFs.Fim Is Null and CadastroTDPFs.Fiscal=Alocacoes.Fiscal
                     and CadastroTDPFs.TDPF=Alocacoes.TDPF and CadastroTDPFs.TDPF=TDPFS.Codigo and Alocacoes.Desalocacao Is Null 
                     and TDPFS.Encerramento Is Null"""       
        cursor.execute(comando, (chaveFiscal,))
        rows = cursor.fetchall()
        if rows==None or len(rows)==0:
            tam = 0
        else:    
            tam = len(rows)
        if tam==0:
            resposta = "0600"
            enviaResposta(resposta, c)  
            conn.close()
            return             
        if tam>=100: #limite de 99 tdpfs
            nn = "99"
            tam = 99
        else:
            nn = str(tam).rjust(2,"0")
        registro = "" 
        resposta = "06"+nn
        i = 0
        total = 0
        for row in rows:
            chaveTdpf = row[0]
            tdpf = row[1]
            nome = row[2]
            if nome==None:
                nome = ""            
            nome = nome[:tamNome].ljust(tamNome)
            vencimento = row[3]
            if vencimento==None:
                vencimento = "00/00/0000"
            else:
                vencimento = vencimento.strftime("%d/%m/%Y")  
            emissao = row[4]
            if emissao==None:
                emissao = "00/00/0000"
            else:
                emissao = emissao.strftime("%d/%m/%Y")                        
            comando = "Select Data, Documento, Vencimento from Ciencias Where TDPF=%s order by Data DESC"
            cursor.execute(comando, (chaveTdpf,))
            cienciaReg = cursor.fetchone() #busca a data de ciência mais recente (DESC acima)
            documento = ""
            documento = documento.ljust(70)             
            if cienciaReg: 
                if cienciaReg[1]!=None:
                    documento = cienciaReg[1].ljust(70)               
                ciencia = cienciaReg[0] #obtem a data de ciência mais recente
                if ciencia!=None:
                    cienciaStr = ciencia.strftime('%d/%m/%Y')                    
                    registro = registro + tdpf + nome + emissao + vencimento + cienciaStr + documento + dataTexto(cienciaReg[2])  
                else:
                    registro = registro + tdpf + nome + emissao + vencimento + "00/00/0000" + documento + "00/00/0000"        
            else:
                registro = registro + tdpf + nome + emissao + vencimento + "00/00/0000" + documento + "00/00/0000"
            i+=1
            total+=1
            if i==5 or total==tam: #de cinco em cinco ou no último registro, enviamos
                enviaRespostaSemFechar(resposta+registro, c)
                resposta = "06"
                registro = ""
                i = 0
                if total==tam:
                    c.close()
                    break #percorreu os registros ou 99 deles, que é o limite
                if total<tam: #ainda não chegou ao final - aguardamos a requisição da continuação
                    try:
                        mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao
                        requisicao = descriptografa(mensagemRec, addr, c)
                        if requisicao!="0612345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (6B)"
                            enviaResposta(resposta, c)    
                            conn.close()
                            return
                    except:
                        c.close()
                        logging.info("Erro de time out 6 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                        conn.close()
                        return
                    
    if codigo==7: #requer e-mail cadastrado
        if len(msgRecebida)>(13+tamChave): #despreza
            resposta = "99REQUISIÇÃO INVÁLIDA (7)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        #a variável row foi buscada na pequena rotina antes do código validador previo de 2 a 5
        email = row[4]
        if email==None or email=="":
            resposta = "07N"
            enviaResposta(resposta, c)  
            conn.close()
            return
        email = email[:50].ljust(50)
        resposta = "07S"+email
        enviaResposta(resposta, c)    
        conn.close()
        return

    if codigo==8: #cadastra ou substitui e-mail        
        if len(msgRecebida)!=(63+tamChave): 
            resposta = "99REQUISIÇÃO INVÁLIDA (8)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        email = msgRecebida[(13+tamChave):].strip()
        if "@" in email:
            if not verificaEMail(email):
                msg = "Email inválido"
                msg = msg.ljust(100)            
                resposta = "08N"+msg
                enviaResposta(resposta, c)
                conn.close()
                return
            elif email[-11:]!="@rfb.gov.br":
                msg = "Email não é institucional"
                msg = msg.ljust(100)            
                resposta = "08N"+msg
                enviaResposta(resposta, c)
                conn.close()
                return                       
        else:
            email = email + "@rfb.gov.br"
        if len(email)<15:
            msg = "Email inválido (nome de usuário curto)"
            msg = msg.ljust(100)            
            resposta = "08N"+msg
            enviaResposta(resposta, c)
            conn.close()
            return                
        comando = "Update Usuarios Set email=%s Where CPF=%s and Saida Is Null"
        try:
            cursor.execute(comando, (email, cpf))
            conn.commit()
            msg = "Email atualizado"
            msg = msg.ljust(100)
            resposta = "08S"+msg
        except:
            msg = "Erro na atualização de tabelas (2)"
            msg = msg.ljust(100)            
            resposta = "08N"+msg
        enviaResposta(resposta, c)
        conn.close()
        return
     
    if codigo==9: #apaga e-mail
        if len(msgRecebida)>(13+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (9)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        comando = "Update Usuarios Set email=Null Where CPF=%s and Saida Is Null"
        try:
            cursor.execute(comando, (cpf,))
            conn.commit()
            msg = "Email excluido"
            msg = msg.ljust(100)
            resposta = "09S"+msg
        except:
            msg = "Erro na atualização da tabela"
            msg = msg.ljust(100)            
            resposta = "09N"+msg
        enviaResposta(resposta, c)
        conn.close()
        return
    
    if codigo==10: #solicita prazos vigentes
        if len(msgRecebida)!=(13+tamChave): #despreza
            resposta = "99REQUISIÇÃO INVÁLIDA (10)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        #a variável row foi buscada na pequena rotina antes do código validador previo de 2 a 5
        d1 = str(row[5]).rjust(2,"0")
        d2 = str(row[6]).rjust(2,"0")
        d3 = str(row[7]).rjust(2,"0")
        resposta = "10"+d1+d2+d3
        enviaResposta(resposta, c)
        conn.close()
        return
    
    if codigo==11: #altera prazos
        if len(msgRecebida)!=(19+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (11)"
            enviaResposta(resposta, c) 
            conn.close()
            return                    
        d1 = msgRecebida[-6:][:2]
        d2 = msgRecebida[-4:][:2]
        d3 = msgRecebida[-2:]
        logging.info(d1)
        logging.info(d2)
        logging.info(d3)
        logging.info("---------")
        if not d1.isdigit() or not d2.isdigit() or not d3.isdigit():
            msg = "Dias devem ser numéricos"
            msg = msg.ljust(100)
            resposta = "11N"+msg
            enviaResposta(resposta, c)  
            conn.close()
            return

        d = [int(d1), int(d2), int(d3)]
        erro = False
        for dia in d:
            if dia<1 or dia>50:
                erro = True
                break
        if erro:
            msg = "Dias devem estar na faixa de 1 a 50"
            msg = msg.ljust(100)
            resposta = "11N"+msg
            enviaResposta(resposta, c)  
            conn.close()
            return

        d.sort(reverse = True)
        d1 = d[0]
        d2 = d[1]
        d3 = d[2]
        comando = "Update Usuarios set d1=%s, d2=%s, d3=%s where CPF=%s and Saida Is Null"
        try:
            cursor.execute(comando, (d1, d2, d3, cpf))
            conn.commit()
            msg = "Alteração efetuada"
            msg = msg.ljust(100)
            resposta = "11S"+msg        
        except:
            conn.rollback()
            msg = "Erro na atualização da tabela"
            msg = msg.ljust(100)
            resposta = "11N"+msg
        enviaResposta(resposta, c)     
        conn.close()
        return            

    if codigo==12: #Relação de TDPFs alocados ao CPF - indica se está sendo monitorado (se estiver em andamento) e se o cpf é supervisor
        if len(msgRecebida)>(14+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (12A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        encerrados = msgRecebida[-1:]
        if encerrados=="N":
            comando = """Select TDPFS.Numero, Alocacoes.Supervisor, TDPFS.Nome, TDPFS.Codigo, TDPFS.DCC, TDPFS.Encerramento, TDPFS.NI from Alocacoes, TDPFS 
                    Where Alocacoes.Fiscal=%s and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo and 
                    TDPFS.Encerramento Is Null Order by TDPFS.Numero"""
        elif encerrados=="S":
            comando = """Select TDPFS.Numero, Alocacoes.Supervisor, TDPFS.Nome, TDPFS.Codigo, TDPFS.DCC, TDPFS.Encerramento, TDPFS.NI from Alocacoes, TDPFS 
                    Where Alocacoes.Fiscal=%s and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo and
                    TDPFS.Encerramento Is Not Null Order by TDPFS.Encerramento DESC, TDPFS.Numero ASC""" 
        else:
            resposta = "99INDICADOR DE ENCERRAMENTO INVÁLIDO (12B)"
            enviaResposta(resposta, c) 
            conn.close()
            return                        
        cursor.execute(comando, (chaveFiscal,))
        rows = cursor.fetchall()
        tam = len(rows)
        if tam==0:
            resposta = "1200"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if tam>=100: #limite de 99 tdpfs
            nn = "99"
            tam = 99
        else:
            nn = str(tam).rjust(2,"0")         
        registro = "" 
        resposta = "12"+nn
        i = 0
        total = 0            
        for row in rows:
            tdpf = row[0]
            nome = row[2]
            supervisor = row[1]
            chaveTdpf = row[3]
            if nome==None or nome=="":
                nome = "ND"
            if supervisor==None or supervisor=="":
                supervisor = "N"
            dcc = row[4]
            if dcc==None:
                dcc = ""
            dcc = dcc.ljust(17)
            encerramento = row[5]
            if encerramento==None:
                encerramento = "00/00/0000"
            else:
                encerramento = encerramento.strftime("%d/%m/%Y")  
            niFiscalizado = row[6]
            if niFiscalizado==None:
                niFiscalizado = ""
            niFiscalizado = niFiscalizado.ljust(18)  
            nome = nome[:tamNome].ljust(tamNome)  
            registro = registro + tdpf + nome + niFiscalizado
            if encerrados=="N":             
                comando = "Select Inicio, Fim from CadastroTDPFs Where Fiscal=%s and TDPF=%s"
                cursor.execute(comando, (chaveFiscal, chaveTdpf))
                linha = cursor.fetchone()
                if linha:
                    if linha[1]==None:
                        registro = registro+"S"
                    else:
                        registro = registro+"N"
                else:
                    registro = registro+"N"
            registro = registro + supervisor + dcc + ("" if encerrados=="N" else encerramento)
            i+=1
            total+=1
            if i==5 or total==tam: #de cinco em cinco ou no último registro, enviamos
                enviaRespostaSemFechar(resposta+registro, c)
                resposta = "12"
                registro = ""
                i = 0
                if total==tam:
                    c.close()
                    return #percorreu os registros ou 99 deles, que é o limite
                if total<tam: #ainda não chegou ao final - aguardamos a requisição da continuação
                    try:
                        mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao
                        requisicao = descriptografa(mensagemRec, addr, c)
                        if requisicao!="1212345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (12C)"
                            enviaResposta(resposta, c) 
                            conn.close()
                            return
                    except:
                        c.close()
                        conn.close()
                        logging.info("Erro de time out 12 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                        return 

    if codigo==13: #mostra lista de tdpfs ativos e últimas ciências sob supervisão do CPF (tipo Orgao R ou N contam como supervisores) - semelhante ao código 6
        qtdeRegistros = 200 #qtde de registros de tdpfs que enviamos por vez
        if len(msgRecebida)!=(19+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (13A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        encerrados = msgRecebida[-6:-5]
        if not encerrados in ["S", "N"]:
            resposta = "99INDICADOR DE ENCERRAMENTO INVÁLIDO (13B)"
            enviaResposta(resposta, c) 
            conn.close()
            return                   
        regInicial = msgRecebida[-5:]
        if not regInicial.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA (13C)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        try:
            regInicial = int(regInicial)            
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA (13D)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        c.settimeout(20)            
        if regInicial>0: #se foi informado o registro, devemos buscar a partir dele, limitado a CINCO
            offsetReg = "Limit "+str(qtdeRegistros)+" Offset "+str(regInicial-1)             
        else: #caso contrário, buscamos todos para informar a quantidade total que existe, mas só enviamos 5 (conforme if ao final do for abaixo)
             offsetReg = "Limit "+str(qtdeRegistros)+" Offset 0"
        logging.info("Offset: "+offsetReg) 
        comando = """Select TDPFS.Numero, TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao, TDPFS.Codigo, TDPFS.DCC, TDPFS.Encerramento, TDPFS.Porte, 
                     TDPFS.Acompanhamento, TDPFS.TrimestrePrevisto, TDPFS.Grupo """
        if tipoOrgaoUsuario=="L": #esta variável e orgaoUsuario vem de rotina comum de validação do usuário
            if encerrados=="N": 
                comando = comando + """from TDPFS, Supervisores 
                                       Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Null 
                                       Order by TDPFS.Numero """+offsetReg
                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    consulta = """Select Count(TDPFS.Numero)
                                  from TDPFS, Supervisores 
                                  Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Null"""
            else:
                comando = comando + """from TDPFS, Supervisores 
                                       Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Not Null 
                                       Order by TDPFS.Encerramento DESC, TDPFS.Numero ASC """+offsetReg            
                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    consulta = """Select Count(TDPFS.Numero)
                                  from TDPFS, Supervisores 
                                  Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Not Null"""
        elif tipoOrgaoUsuario=="R":
            if encerrados=="N": 
                if chaveFiscal==None or chaveFiscal==0: 
                    comando = comando + """from TDPFS
                                           Where TDPFS.Encerramento Is Null and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s)
                                           Order by TDPFS.Numero """+offsetReg
                else:
                    comando = comando + """from TDPFS
                                           Where TDPFS.Encerramento Is Null and 
                                           (TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) or 
                                           TDPFS.Grupo in (Select Equipe from Supervisores Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null))
                                           Order by TDPFS.Numero """+offsetReg

                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    if chaveFiscal==None or chaveFiscal==0: 
                        consulta = """Select Count(TDPFS.Numero)
                                      from TDPFS
                                      Where TDPFS.Encerramento Is Null and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) """
                    else:
                        consulta = """Select Count(TDPFS.Numero)
                                      from TDPFS
                                      Where TDPFS.Encerramento Is Null and (TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) or 
                                      TDPFS.Grupo in (Select Equipe from Supervisores Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null)) """                        
            else: #encerrados
                if chaveFiscal==None or chaveFiscal==0: 
                    comando = comando + """from TDPFS
                                           Where TDPFS.Encerramento Is Not Null and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s)
                                           Order by TDPFS.Encerramento DESC, TDPFS.Numero """+offsetReg
                else:
                    comando = comando + """from TDPFS
                                           Where TDPFS.Encerramento Is Not Null and 
                                           (TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) or 
                                           TDPFS.Grupo in (Select Equipe from Supervisores Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null))
                                           Order by TDPFS.Encerramento DESC, TDPFS.Numero """+offsetReg

                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    if chaveFiscal==None or chaveFiscal==0: 
                        consulta = """Select Count(TDPFS.Numero)
                                      from TDPFS
                                      Where TDPFS.Encerramento Is Not Null and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) """
                    else:
                        consulta = """Select Count(TDPFS.Numero)
                                      from TDPFS
                                      Where TDPFS.Encerramento Is Not Null and (TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) or 
                                      TDPFS.Grupo in (Select Equipe from Supervisores Where Supervisores.Fiscal=%s and Supervisores.Fim Is Null)) """             
        elif tipoOrgaoUsuario=="N":
            if encerrados=="N": 
                comando = comando + """from TDPFS
                                       Where TDPFS.Encerramento Is Null  
                                       Order by TDPFS.Numero """+offsetReg
                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    consulta = """Select Count(TDPFS.Numero)
                                  from TDPFS
                                  Where TDPFS.Encerramento Is Null """
            else:
                comando = comando + """from TDPFS
                                       Where TDPFS.Encerramento Is Not Null  
                                       Order by TDPFS.Encerramento DESC, TDPFS.Numero ASC """+offsetReg            
                if regInicial==0: #contamos a quantidade de registros para informar na primeira consulta
                    consulta = """Select Count(TDPFS.Numero)
                                from TDPFS
                                Where TDPFS.Encerramento Is Not Null """             

        if regInicial==0:
            if tipoOrgaoUsuario=="L": 
                cursor.execute(consulta, (chaveFiscal,))
            elif tipoOrgaoUsuario=="R":
                if chaveFiscal==None or chaveFiscal==0:
                    cursor.execute(consulta, (orgaoUsuario,))
                else:
                    cursor.execute(consulta, (orgaoUsuario, chaveFiscal))
            else:
                cursor.execute(consulta)
            totalReg = cursor.fetchone()
            #print(str(totalReg[0]))
            if totalReg:
                tam = totalReg[0]
            else:
                tam = 0
            if tam==0:
                resposta = "1300000" #13+qtde TDPFs
                enviaResposta(resposta, c) 
                conn.close()
                return             
            if tam>=100000: #limite de  tdpfs
                nnnnn = "99999"
                tam = 99999
            else:
                nnnnn = str(tam).rjust(5, "0")
            #resposta = "13"+nnnn #código da resposta e qtde de TDPFs que serão retornados
        #else:
        #    resposta = "13"
        if tipoOrgaoUsuario=="L":
            cursor.execute(comando, (chaveFiscal,))
        elif tipoOrgaoUsuario=="R":
            if chaveFiscal==None or chaveFiscal==0:
                cursor.execute(comando, (orgaoUsuario,))  
            else:
                cursor.execute(comando, (orgaoUsuario, chaveFiscal))  
        else:
            cursor.execute(comando)                      
        rows = cursor.fetchall()   
        if regInicial>0:
            tam = len(rows)     
        registro = "" 
        i = 0
        total = 0         
        for row in rows:
            tdpf = row[0]          
            nome = row[1]
            if nome==None:
                nome = ""   
            nome = nome[:tamNome].ljust(tamNome)  
            vencimento = dataTexto(row[2])                      
            emissao = dataTexto(row[3])  
            chaveTdpf = row[4]  
            retorno = consultaPontuacao(cursor, chaveTdpf, tipoOrgaoUsuario, False, True)
            if retorno==None:
                pontos = "0000"
            else:
                pontos = retorno[4:8]              
            dcc = row[5]
            if dcc==None:
                dcc = ""
            dcc = dcc.ljust(17) 
            encerramento = row[6]
            if encerramento==None:
                encerramento = "00/00/0000"         
            else:
                encerramento = encerramento.strftime("%d/%m/%Y")
            porte = row[7]
            acompanhamento = row[8]
            trimestre = row[9]
            if trimestre==None:
                trimestre = " ".ljust(6)
            elif len(trimestre)!=6:
                trimestre = " ".ljust(6)
            if porte==None or porte=="":
                porte = "ND "
            if acompanhamento==None or acompanhamento=="":
                acompanhamento = "N"
            equipe = row[10]
            if equipe==None:
                equipe =""
            equipe = equipe.ljust(25)
            #busca a primeira ciência e a última
            comando = "Select Data, Documento from Ciencias Where TDPF=%s order by Data"
            cursor.execute(comando, (chaveTdpf,))
            cienciaRegs = cursor.fetchall() #busca a data de ciência mais recente (DESC acima)
            if cienciaRegs: 
                primCiencia = dataTexto(cienciaRegs[0][0]) #obtem a primeira data de ciência
                ultCiencia =  dataTexto(cienciaRegs[len(cienciaRegs)-1][0]) #obtém a última data de ciência (mais recente)
                documento = cienciaRegs[len(cienciaRegs)-1][1] #documento da última data de ciência
                if documento==None:
                    documento = ""
                documento = documento.ljust(70)                                   
                registro = registro + tdpf + nome + emissao + vencimento + dcc + primCiencia + ultCiencia + documento 
            else:
                registro = registro + tdpf + nome + emissao + vencimento + dcc + "00/00/0000" + "00/00/0000" + " ".ljust(70) #provavelmente nenhum fiscal iniciou monitoramento
            #verifica se o TDPF está sendo monitorado 
            if encerrados=="N":
                comando = "Select * from CadastroTDPFs Where TDPF=%s and Fim Is Null"   
                cursor.execute(comando, (chaveTdpf,))
                linha = cursor.fetchone()
                if linha:
                    registro = registro+"S"
                else:
                    registro = registro+"N"
            #busca o fiscal alocado há mais tempo no TDPF  
            comando = """Select Fiscais.Nome, Alocacoes.Alocacao 
                         from Fiscais, Alocacoes 
                         Where Alocacoes.Desalocacao Is Null and Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.TDPF=%s Order by Alocacoes.Alocacao"""
            cursor.execute(comando, (chaveTdpf,))
            linha = cursor.fetchone()
            if linha:
                nomeFiscal = linha[0]
                if nomeFiscal==None:
                    nomeFiscal = "NÃO ENCONTRADO NO REGISTRO"
            else:
                nomeFiscal = "NÃO ENCONTRADO"
            nomeFiscal = nomeFiscal[:100].ljust(100)   
            registro = registro+nomeFiscal+pontos+porte+acompanhamento+trimestre+equipe
            registro = registro + ("" if encerrados=="N" else encerramento)
            #logging.info(registro)
            total+=1
            i+=1
            if i%qtdeRegistros==0 or total==tam: #de qtdeRegistros em qtdeRegistros ou no último registro enviamos a mensagem
                if regInicial==0:
                    #tamanhoReg = str(len(nnnnn+registro)).rjust(5, "0")
                    resposta = "13"+nnnnn #tamanhoReg+nnnnn                  
                else:
                    #tamanhoReg = str(len(registro)).rjust(5, "0")
                    resposta = "13"#+tamanhoReg 
                enviaResposta(resposta+registro, c)
                return 
                
    if codigo==14: #inclui atividade em um tdpf
        if len(msgRecebida)!=(113+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (14A)"
            enviaResposta(resposta, c) 
            conn.close()
            return          
        atividade = msgRecebida[-84:-34].strip()     
        if len(atividade)<4:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE - DESCRIÇÃO CURTA (14C)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        inicio = msgRecebida[-34:-24]
        try:
            inicio = datetime.strptime(inicio, "%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO INVÁLIDA (14D)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if inicio.date()>datetime.today().date():  
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO FUTURA (14D1)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if inicio.date()<emissao.date(): #ciência não pode ser inferior à data de emissão (obtida nas verificacões gerais)
            resposta = "14Data de início da atividade não pode ser inferior à de emissão do TDPF ("+emissao.strftime("%d/%m/%Y")+")"
            enviaResposta(resposta, c)  
            conn.close()
            return                                               
        vencimento = msgRecebida[-24:-14]
        try:
            vencimento = datetime.strptime(vencimento,"%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO INVÁLIDA (14E)"
            enviaResposta(resposta, c) 
            conn.close()
            return        
        if inicio>vencimento or vencimento.date()<datetime.today().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO ANTERIOR À DE INÍCIO OU PASSADA (14F)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        termino = msgRecebida[-14:-4]
        horas = msgRecebida[-4:-1]
        if termino!="00/00/0000":
            try:
                terminoAux = datetime.strptime(termino, "%d/%m/%Y")
            except:
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO INVÁLIDA (14G)"
                enviaResposta(resposta, c) 
                conn.close()
                return        
            if inicio>terminoAux or terminoAux.date()>datetime.today().date():
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO ANTERIOR À DE INÍCIO OU FUTURA (14H)"
                enviaResposta(resposta, c) 
                conn.close()
                return
        else:
            terminoAux = None
        if not horas.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (14I)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            horas = int(horas)   
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (14J)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if horas<0:
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (14K)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        haObservacoes = msgRecebida[-1:]
        if haObservacoes=="S":
            enviaRespostaSemFechar("1400", c) #envia este para que o cliente envie as observações
            try:
                mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao criptografada
                requisicao = descriptografa(mensagemRec, addr, c)
                if requisicao[:2]!="14":
                    resposta = "99REQUISIÇÃO INVÁLIDA (14L)"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return
                observacoes = requisicao[2:102].strip()
            except:
                c.close()
                conn.close()
                logging.info("Erro de time out 14 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                return            
        else:
            observacoes = ""
        #já foi verificado se está alocado
        #verificamos se o tdpf existe e se é de responsabilidade do usuário e está em andamento
        #if not verificaAlocacao(conn, cpf, tdpf):
        #    resposta = "14CPF NÃO ESTÁ ALOCADO OU NÃO É SUPERVISOR OU TDPF ENCERRADO"
        #    enviaResposta(resposta, c) 
        #    conn.close()
        #    return 
        #podemos incluir a atividade 
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf) #tdpf e cpf vêm da rotina comum às requisições 2-5 e 14-17
        try:
            comando = "Insert Into Atividades (TDPF, Atividade, Inicio, Vencimento, Termino, Horas, Observacoes) Values (%s, %s, %s, %s, %s, %s, %s)"           
            cursor.execute(comando, (chaveTdpf, atividade, inicio, vencimento, terminoAux, horas, observacoes))  #chaveTdpf vem da rotina comum às requisições 2-5 e 14-17
            resposta = "14REGISTRO INCLUÍDO"            
            if tdpfMonitorado and monitoramentoAtivo==False: #monitoramento do tdpf estava desativado - ativa
                comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                cursor.execute(comando, (chave,))  
                resposta = resposta + " - MONITORAMENTO REATIVADO"                       
            elif not tdpfMonitorado: #tdpf não estava sendo monitorado - inclui ele
                comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date())) #chaveFiscal vem para todas as funções; chaveTdpf vem da rotina comum às requisições 2-5 e 14-17
                resposta = resposta + " - MONITORAMENTO INICIADO"  
            else:
                resposta = resposta + " - TDPF MONITORADO"                  
            conn.commit()
        except:
            conn.rollback()
            resposta = "14ERRO NA INCLUSÃO DO REGISTRO"
        enviaResposta(resposta, c)  
        conn.close()
        return             


    if codigo==15: #mostra lista de atividades de um tdpf - cpf deve estar alocado ou ser supervisor (já foi verificado)
        #if len(msgRecebida)>(13+tamChave): #despreza
        #    pass
        if len(msgRecebida)!=(32+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (15A)"
            enviaResposta(resposta, c) 
            conn.close()
            return         
        regInicial = msgRecebida[-3:]
        if not regInicial.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA (15B)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        try:
            regInicial = int(regInicial)            
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA (15C)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if regInicial>0: #se foi informado o registro, devemos buscar a partir dele, limitado a dez
            offsetReg = "Limit 10 Offset "+str(regInicial-1)             
        else: #caso contrário, buscamos todos para informar a quantidade que há 
             offsetReg = ""  
        comando = "Select Codigo, Atividade, Vencimento, Inicio, Termino, Horas, Observacoes from Atividades Where TDPF=%s Order by Inicio "+offsetReg
        cursor.execute(comando, (chaveTdpf,))
        rows = cursor.fetchall()
        if rows:
            tam = len(rows)
        else:
            tam = 0           
        if tam==0:
            resposta = "15000" 
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if tam>=1000: #limite de 999 atividades
            nnn = "999"
            tam = 999
        else:
            nnn= str(tam).rjust(3, "0")
        registro = "" 
        if regInicial==0:
            resposta = "15"+nnn #código da resposta e qtde de TDPFs que serão retornados
        else:
            resposta = "15"     
        total = 0
        i = 0           
        for row in rows:
            codigoAtiv = str(row[0]).rjust(10, "0")
            atividade = row[1].ljust(50)
            vencimentoAtiv = dataTexto(row[2])
            inicio = dataTexto(row[3])
            termino = dataTexto(row[4])  
            horas = row[5]    
            if horas==None:
                horas = 0                            
            horas = str(horas)[:3].rjust(3, "0")
            observacoes = row[6] if row[6]!=None else ""
            registro = registro + codigoAtiv + atividade + inicio + termino + vencimentoAtiv + horas + observacoes.ljust(100)
            total+=1
            i+=1
            if i%10==0 or total==tam: #de 10 em 10 ou no último registro enviamos a mensagem
                enviaResposta(resposta+registro, c)
                return      

    if codigo==16: #apaga atividade em um tdpf
        if len(msgRecebida)!=(39+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (16A)"
            enviaResposta(resposta, c) 
            conn.close()
            return         
        codAtividade = msgRecebida[-10:].strip()     
        if not codAtividade.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ATIVIDADE NÃO NUMÉRICO (16C)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        try:
            codAtividade = int(codAtividade)
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ATIVIDADE INVÁLIDO (16D)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if codAtividade<0:  
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ATIVIDADE NEGATIVO (16E)"
            enviaResposta(resposta, c) 
            conn.close()
            return                                  
        #já foi verificado
        #verificamos se o tdpf existe e se é de responsabilidade do usuário e está em andamento
        #if not verificaAlocacao(conn, cpf, tdpf):
        #    resposta = "16CPF NÃO ESTÁ ALOCADO OU TDPF ENCERRADO OU INEXISTENTE"
        #    enviaResposta(resposta, c) 
        #    conn.close()
        #    return
        #verificamos se a atividade existe e é do TDPF
        comando = "Select Codigo, TDPF From Atividades Where Codigo=%s" 
        cursor.execute(comando, (codAtividade,))
        row = cursor.fetchone()
        bAchou = False
        if row:
            if len(row)>0:
                bAchou = True
        if not bAchou:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE NÃO ENCONTRADA(16G)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if row[1]!=chaveTdpf:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE NÃO PERTENCE AO TDPF (16H)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)  
        if not tdpfMonitorado or monitoramentoAtivo==False:
            resposta = "16TDPF NÃO ESTÁ SENDO MONITORADO PELO USUÁRIO - EXCLUSÃO NÃO PERMITIDA"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        #podemos excluir a atividade 
        comando = "Delete from Atividades Where Codigo=%s"
        try:
            cursor.execute(comando, (codAtividade,))
            conn.commit()
            resposta = "16REGISTRO EXCLUÍDO"
        except:
            conn.rollback()
            resposta = "16ERRO NA EXCLUSÃO DO REGISTRO"
        enviaResposta(resposta, c)  
        conn.close()
        return             

    if codigo==17: #altera atividade de um tdpf
        if len(msgRecebida)!=(123+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (17A)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        codAtividade = msgRecebida[-94:-84].strip()     
        if not codAtividade.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ATIVIDADE NÃO NUMÉRICO (17C)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        try:
            codAtividade = int(codAtividade)
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ATIVIDADE INVÁLIDO (17D)"
            enviaResposta(resposta, c) 
            conn.close()
            return                   
        atividade = msgRecebida[-84:-34].strip()     
        if len(atividade)<4:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE - DESCRIÇÃO CURTA (17E)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        inicio = msgRecebida[-34:-24]
        try:
            inicio = datetime.strptime(inicio, "%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO INVÁLIDA (17F)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if inicio.date()>datetime.today().date():  
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO FUTURA (17G)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if inicio.date()<emissao.date(): #ciência não pode ser inferior à data de emissão (obtida nas verificacões gerais)
            resposta = "17Data de início da atividade não pode ser inferior à de emissão do TDPF ("+emissao.strftime("%d/%m/%Y")+")"
            enviaResposta(resposta, c)  
            conn.close()
            return                                            
        vencimento = msgRecebida[-24:-14]
        try:
            vencimento = datetime.strptime(vencimento,"%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO INVÁLIDA (17H)"
            enviaResposta(resposta, c) 
            conn.close()
            return        
        if inicio>vencimento or vencimento.date()<datetime.today().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO ANTERIOR À DE INÍCIO OU PASSADA (17I)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        termino = msgRecebida[-14:-4]
        if termino!="00/00/0000":
            try:
                terminoAux = datetime.strptime(termino, "%d/%m/%Y")
            except:
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO INVÁLIDA (17J)"
                enviaResposta(resposta, c) 
                conn.close()
                return        
            if inicio>terminoAux or terminoAux.date()>datetime.today().date():
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO ANTERIOR À DE INÍCIO OU FUTURA (17K)"
                enviaResposta(resposta, c) 
                conn.close()
                return
        else:
            terminoAux = None
        horas = msgRecebida[-4:-1]
        if not horas.isdigit() and horas!="AAA":
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (17L)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            horas = int(horas)   
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (17M)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if horas<0:
            resposta = "99REQUISIÇÃO INVÁLIDA - HORAS INVÁLIDAS (17N)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        haObservacoes = msgRecebida[-1:]
        if haObservacoes=="S":
            enviaRespostaSemFechar("1700", c) #envia este para que o cliente envie as observações
            try:
                mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao criptografada
                requisicao = descriptografa(mensagemRec, addr, c)
                if requisicao[:2]!="17":
                    resposta = "99REQUISIÇÃO INVÁLIDA (17O)"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return
                observacoes = requisicao[2:102].strip()
            except:
                c.close()
                conn.close()
                logging.info("Erro de time out 17 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                return            
        else:
            observacoes = ""
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)  
        if not tdpfMonitorado or monitoramentoAtivo==False:
            resposta = "17TDPF NÃO ESTÁ SENDO MONITORADO PELO USUÁRIO - ALTERAÇÃO NÃO PERMITIDA"
            enviaResposta(resposta, c) 
            conn.close()
            return                                             
        #já foi verificado
        #verificamos se o tdpf existe e se é de responsabilidade do usuário e está em andamento
        #if not verificaAlocacao(conn, cpf, tdpf):
        #    resposta = "17CPF NÃO ESTÁ ALOCADO AO TDPF OU TDPF ENCERRADO/INEXISTENTE"
        #    enviaResposta(resposta, c) 
        #    conn.close()
        #    return 
        #podemos alterar a atividade 
        try:
            comando = "Update Atividades Set TDPF=%s, Atividade=%s, Inicio=%s, Vencimento=%s, Termino=%s, Horas=%s, Observacoes=%s Where Codigo=%s"           
            cursor.execute(comando, (chaveTdpf, atividade, inicio, vencimento, terminoAux, horas, observacoes, codAtividade))                
            conn.commit()
            resposta = "17ALTERAÇÃO EFETIVADA"
        except:
            conn.rollback()
            resposta = "17ERRO NA ALTERAÇÃO"
        enviaResposta(resposta, c)  
        conn.close()
        return             

    if codigo==18: #inclui entrada em diário da fiscalização
        if len(msgRecebida)!=(33+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (18A)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        primEnvio = msgRecebida[-4:-2]
        numPartes = msgRecebida[-2:]
        try:
            primEnvio = int(primEnvio) 
            if primEnvio!=0:       
                resposta = "99REQUISIÇÃO INVÁLIDA - CÓDIGO INVÁLIDO DO PRIMEIRO ENVIO (18C)"
                enviaResposta(resposta, c) 
                conn.close()
                return
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓDIGO INVÁLIDO DO PRIMEIRO ENVIO (18D)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        try:
            numPartes = int(numPartes) 
            if primEnvio!=0:       
                resposta = "99REQUISIÇÃO INVÁLIDA - NÚMERO DE PARTES INVÁLIDA (18E)"
                enviaResposta(resposta, c) 
                conn.close()
                return
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - NÚMERO DE PARTES INVÁLIDA (18F)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        resposta = "1801OK"
        respostaErro = "1888"
        entrada = None
        logging.info(msgRecebida)
        c.settimeout(15)  
        for parte in range(1, numPartes+1):    
            enviaRespostaSemFechar(resposta, c)   
            logging.info("Enviou")              
            try:              
                mensagemRec = c.recv(4096) #chegou a requisicao criptografada com certificado do usuário (não há como descriptografar)
                tam = int(mensagemRec[:5].decode('utf-8'))
                mensagemRec = mensagemRec[5:]
                tamEfetivo = len(mensagemRec)
                tentativas = 0
                while tamEfetivo<tam:
                    mensagemRec = c.recv(4096) #chegou a requisicao criptografada com certificado do usuário (não há como descriptografar)
                    tamEfetivo = len(mensagemRec)
                    tentativas+=1
                    if tentativas>30:
                        logging.info("Tentativas de recebimento de parte de uma entrada foram excedidas - CPF "+cpf)   
                        c.close()
                        conn.close()
                        return
            except:
                c.close()
                conn.close()
                logging.info("Erro de time out 18 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                return
            if parte==1:
                entrada = mensagemRec
            else:                 
                entrada = entrada + mensagemRec    
        if entrada:                              
            comando = "Insert into DiarioFiscalizacao (Fiscal, TDPF, Data, Entrada) Values (%s, %s, %s, %s)"
            try:
                logging.info("Inserindo ...")
                cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date(), entrada)) #chaveFiscal vem para todas as funções; chaveTdpf vem da rotina comum às requisições 2-5 e 14-20
                conn.commit()            
                consulta = "Select Codigo from DiarioFiscalizacao Where Fiscal=%s and TDPF=%s Order by Codigo DESC" #obtém a última entrada deste fiscal/tdpf
                cursor.execute(consulta, (chaveFiscal, chaveTdpf))
                registro = cursor.fetchone()
                if registro!=None:
                    chave = registro[0]
                    enviaResposta("1899"+str(chave).rjust(10,"0"), c)  #enviamos a chave do registro incluído para facilitar a atualização e sua exclusão posteriormente
                else:
                    enviaResposta(respostaErro+"INCLUSÃO DA ENTRADA OU CONSULTA FALHOU (18K)", c)                              
            except:
                conn.rollback()
                enviaResposta(respostaErro+"INCLUSÃO DA ENTRADA FALHOU (18L)", c) 
        conn.close()
        return             

    if codigo==19: #solicita entradas do diário da fiscalização
        if len(msgRecebida)!=(31+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (19A)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        primEnvio = msgRecebida[-2:]
        if primEnvio!="00":
            resposta = "99REQUISIÇÃO INVÁLIDA (19B)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        consulta = "Select Codigo, Data from DiarioFiscalizacao Where Fiscal=%s and TDPF=%s order by Codigo"
        cursor.execute(consulta, (chaveFiscal, chaveTdpf))
        entradas = cursor.fetchall()
        if entradas==None:
            numEntradas = 0
        else:
            numEntradas = len(entradas)
        if numEntradas==0:
            enviaResposta("19000NÃO HÁ ENTRADAS CADASTRADAS PARA ESTE TDPF/CPF", c)
            conn.close()
            return
        enviaRespostaSemFechar("19"+str(numEntradas).rjust(3, "0")+"ENTRADAS DISPONÍVEIS", c)
        connRaw = conectaRaw() #para recuperar as entradas (varbinary)
        if connRaw==None:
            resposta = "97ERRO DE CONEXÃO AO BD P/ RECUPERAR ENTRADAS (RAW)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        cursorRaw = connRaw.cursor(buffered=True)
        consulta = "Select Entrada from DiarioFiscalizacao Where Fiscal=%s and TDPF=%s order by Codigo"                        
        cursorRaw.execute(consulta, (chaveFiscal, chaveTdpf))
        entradasRaw = cursorRaw.fetchall()
        j = 0
        tamParte = 4000 #tamanho de cada parte da entrada enviada separadamente
        for entrada in entradas:
            c.settimeout(20)
            mensagemRec = c.recv(1024).decode('utf-8') #chegou a requisicao sem criptografia            
            if mensagemRec[:2]!="19" or mensagemRec[2:4]!="11":
                enviaResposta("99REQUISIÇÃO INVÁLIDA - AGUARDANDO PEDIDOS DE CONTINUAÇÃO (19C)", c) 
                conn.close()
                return             
            codReg = entrada[0]   
            data = entrada[1].strftime("%d/%m/%Y") 
            texto = entradasRaw[j][0]   
            j+=1              
            totalPartes = len(texto) // tamParte #tamParte caracteres do texto são enviados de cada vez
            if (len(texto) % tamParte) > 0:
                totalPartes+=1
            enviaRespostaSemFechar("19"+str(codReg).rjust(10, "0")+data+str(totalPartes).rjust(2, "0"), c)
            for i in range(totalPartes): #para cada entrada (codReg), fazemos o envio das partes de 300 caracteres cada
                try:
                    mensagemRec = c.recv(1024).decode('utf-8') #chegou a requisicao sem criptografia
                    if mensagemRec==None:
                        mensagemRec = "NULO"
                except:
                    c.close()
                    conn.close()
                    connRaw.close()
                    logging.info("Erro de time out 19 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                    return                     
                if mensagemRec[:2]!="19" or mensagemRec[2:4]!="11":
                    enviaResposta("99REQUISIÇÃO INVÁLIDA - AGUARDANDO PEDIDOS DE CONTINUAÇÃO (19D)", c) 
                    conn.close()
                    connRaw.close()
                    return 
                resposta = texto[i*tamParte:(i*tamParte+tamParte)]
                c.sendall(str(len(resposta)).rjust(5,"0").encode('utf-8')+resposta) #enviamos o tamanho da resposta (5 primeiros) e o diário criptografado
        c.close()
        conn.close()
        connRaw.close()
        return

    if codigo==20: #apaga entrada de um diário
        if len(msgRecebida)!=(39+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (20A)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        #print(msgRecebida)      
        codEntrada = msgRecebida[-10:].strip()     
        if not codEntrada.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ENTRADA NÃO NUMÉRICO (20B)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        try:
            codEntrada = int(codEntrada)
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ENTRADA INVÁLIDO (20C)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if codEntrada<0:  
            resposta = "99REQUISIÇÃO INVÁLIDA - CÓD ENTRADA NEGATIVO (20D)"
            enviaResposta(resposta, c) 
            conn.close()
            return                                  
        #já foi verificado
        #verificamos se o tdpf existe e se é de responsabilidade do usuário e está em andamento
        #if not verificaAlocacao(conn, cpf, tdpf):
        #    resposta = "16CPF NÃO ESTÁ ALOCADO OU TDPF ENCERRADO OU INEXISTENTE"
        #    enviaResposta(resposta, c) 
        #    conn.close()
        #    return
        #verificamos se a atividade existe e é do TDPF
        comando = "Select Codigo, TDPF From DiarioFiscalizacao Where Codigo=%s" 
        cursor.execute(comando, (codEntrada,))
        row = cursor.fetchone()
        bAchou = False
        if row:
            if len(row)>0:
                bAchou = True
        if not bAchou:
            resposta = "99REQUISIÇÃO INVÁLIDA - ENTRADA NÃO ENCONTRADA(20E)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if row[1]!=chaveTdpf:
            resposta = "99REQUISIÇÃO INVÁLIDA - ENTRADA NÃO PERTENCE AO TDPF (20F)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        #tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)  <-- nesta funcionalidade, é irrelevante o monitoramento
        #if not tdpfMonitorado or monitoramentoAtivo==False:
        #    resposta = "20TDPF NÃO ESTÁ SENDO MONITORADO PELO USUÁRIO - EXCLUSÃO NÃO PERMITIDA"
        #    enviaResposta(resposta, c) 
        #    conn.close()
        #    return             
        #podemos excluir a entrada
        comando = "Delete from DiarioFiscalizacao Where Codigo=%s"
        try:
            cursor.execute(comando, (codEntrada,))
            conn.commit()
            resposta = "2000REGISTRO EXCLUÍDO"
        except:
            conn.rollback()
            resposta = "2099ERRO NA EXCLUSÃO DO REGISTRO"
        enviaResposta(resposta, c)  
        conn.close()
        return             

    if codigo==21: #inclui informação do DCC        
        if len(msgRecebida)!=(46+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (21A)"
            enviaResposta(resposta, c) 
            conn.close()
            return          
        dcc = msgRecebida[-17:].strip()  
        if not dcc.isdigit() and len(dcc)!=0:
            resposta = "99REQUISIÇÃO INVÁLIDA - DCC DEVE SER NUMÉRICO (21B)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if len(dcc)!=17 and len(dcc)!=0: #o dcc pode estar em branco
            resposta = "99REQUISIÇÃO INVÁLIDA - DCC DEVE TER 17 DÍGITOS (21C)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if len(dcc)==17:
            if not verificaDVDCC(dcc):             
                resposta = "99REQUISIÇÃO INVÁLIDA - DCC INVÁLIDO (21D)"
                enviaResposta(resposta, c) 
                conn.close()
                return
            #verificamos se OUTRO tdpf utilizou o número do DCC
            consulta = "Select TDPFS.Codigo, TDPFS.Numero From TDPFS Where DCC=%s"       
            cursor.execute(consulta, (dcc,))
            rows = cursor.fetchall()
            bAchou = False
            for row in rows:
                if row[0]!=chaveTdpf:
                    bAchou = True
                    break
            if bAchou:
                resposta = "21Nº DO DCC ESTÁ EM USO POR OUTRO TDPF - "+row[1]
                enviaResposta(resposta, c) 
                conn.close()
                return         
        #chaveTdpf vêm da rotina comum às requisições 2-5 e 14-21, onde tb foi verifica se o usuário está alocado ou é supervisor
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)
        try:
            if dcc=="":
                dcc = None
            comando = "Update TDPFS Set DCC=%s Where TDPFS.Codigo=%s"           
            cursor.execute(comando, (dcc, chaveTdpf)) 
            if tdpfMonitorado and monitoramentoAtivo==False: #monitoramento do tdpf estava desativado - ativa
                comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                cursor.execute(comando, (chave,)) 
                msgMonitoramento = "MONITORAMENTO DO TDPF REATIVADO"                        
            elif not tdpfMonitorado: #tdpf não estava sendo monitorado - inclui ele
                comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date()))  
                msgMonitoramento = "MONITORAMENTO DO TDPF INICIADO" 
            else:
                msgMonitoramento = "TDPF MONITORADO"                       
            resposta = "21INFORMAÇÃO REGISTRADA - "+msgMonitoramento                               
            conn.commit()
        except:
            conn.rollback()
            resposta = "21ERRO NO REGISTRO DA INFORMAÇÃO"
        enviaResposta(resposta, c)  
        conn.close()
        return   

    if codigo==23: #inclui/atualiza/exclui parâmetros de pontuação do TDPF
        if len(msgRecebida)!=(29+tamChave) and len(msgRecebida)!=(68+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA - TAMANHO DO REGISTRO "+str(len(msgRecebida))+" (23A)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        comando = "Select Encerramento from TDPFS Where Codigo=%s"
        cursor.execute(comando, (chaveTdpf,))          
        row = cursor.fetchone()
        if row[0]!=None:
            if row[0].date()<datetime.now().date()-timedelta(days=180): #TDPF não pode estar encerrado há mais de 180 dias
                resposta = "23V"
                enviaResposta(resposta, c) 
                conn.close()
                return                 
        if len(msgRecebida)==(29+tamChave): #apaga o registro para o TDPF (não vieram as informações dos resultados obtidos)
            comando = "Delete From Resultados Where TDPF=%s"
            try:
                cursor.execute(comando, (chaveTdpf,))
                conn.commit()
                resposta = "23SREGISTRO PORVENTURA EXISTENTE FOI EXCLUÍDO"
            except:                
                conn.rollback()
                resposta = "23NERRO NA EXCLUSÃO DO REGISTRO"
            enviaResposta(resposta, c) 
            conn.close()
            return
        parametros = msgRecebida[29+tamChave:]
        arrolamentos = parametros[:2]
        medCautelar = parametros[2:3]
        rffps = parametros[3:5]
        inaptidoes = parametros[5:7]
        baixas = parametros[7:9]
        excSimples = parametros[9:11]
        sujPassivos = parametros[11:13]
        dvs = parametros[13:16]
        situacao11 = parametros[16:17]
        interposicao = parametros[17:18]
        situacao15 = parametros[18:19] 
        estabPrev1 = parametros[19:22]
        estabPrev2 = parametros[22:24]
        segurados = parametros[24:28]
        prestadores = parametros[28:31]
        tomadores = parametros[31:34]
        qtdePER = parametros[34:36] 
        lancMuldi = parametros[36:37]
        compensacao = parametros[37:38]
        creditoExt = parametros[38:39]   
        try:
            arrolamentos = int(arrolamentos)
            if arrolamentos<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE ARROLAMENTOS INVÁLIDO (23B1)"
                enviaResposta(resposta, c) 
                conn.close()
                return                 
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE ARROLAMENTOS INVÁLIDO (23B)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        try:
            rffps = int(rffps)
            if rffps<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE RFFPS INVÁLIDO (23C1)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE RFFPS INVÁLIDO (23C)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            inaptidoes = int(inaptidoes)
            if inaptidoes<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE INAPTIDÕES INVÁLIDO (23D1)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE INAPTIDÕES INVÁLIDO (23D)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            baixas = int(baixas)
            if baixas<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE BAIXAS INVÁLIDO (23E1)"
                enviaResposta(resposta, c) 
                conn.close()
                return                
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE BAIXAS INVÁLIDO (23E)"
            enviaResposta(resposta, c) 
            conn.close()
            return                                                
        try:
            excSimples = int(excSimples)
            if excSimples<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE EXCLUSÕES DO SIMPLES INVÁLIDO (23F1)"
                enviaResposta(resposta, c) 
                conn.close()
                return            
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE EXCLUSÕES DO SIMPLES INVÁLIDO (23F)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            sujPassivos = int(sujPassivos)
            if sujPassivos<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE SUJEITOS PASSIVOS INVÁLIDO (23G1)"
                enviaResposta(resposta, c) 
                conn.close()
                return                
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE SUJEITOS PASSIVOS INVÁLIDO (23G)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        try:
            dvs = int(dvs)
            if dvs<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE DILIGÊNCIAS VINCULADAS INVÁLIDO (23H1)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE DILIGÊNCIAS VINCULADAS INVÁLIDO (23H)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if not medCautelar in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE MED CAUTELAR INVÁLIDA (23I)"
            enviaResposta(resposta, c) 
            conn.close()
            return       
        if not situacao11 in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE SITUAÇÃO 11 INVÁLIDA (23J)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        if not interposicao in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE INTERPOSIÇÃO INVÁLIDA (23K)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if not situacao15 in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE SITUAÇÃO 15 INVÁLIDA (23L)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            estabPrev1 = int(estabPrev1)
            if estabPrev1<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE ESTAB PREV INVÁLIDO (23L1)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE ESTAB PREV INVÁLIDO (23L2)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            estabPrev2 = int(estabPrev2)
            if estabPrev2<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE CEI/CAEPF/OBRA INVÁLIDO (23L3)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE CEI/CAEPF/OBRA INVÁLIDO (23L4)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            segurados = int(segurados)
            if segurados<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE SEGURADOS INVÁLIDO (23L5)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE SEGURADOS INVÁLIDO (23L6)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            prestadores = int(prestadores)
            if prestadores<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE PRESTADORES INVÁLIDO (23L7)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE PRESTADORES INVÁLIDO (23L8)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            tomadores = int(tomadores)
            if tomadores<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE TOMADORES INVÁLIDO (23L9)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE TOMADORES INVÁLIDO (23L10)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            qtdePER = int(qtdePER)
            if qtdePER<0:
                resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE PERS INVÁLIDO (23L11)"
                enviaResposta(resposta, c) 
                conn.close()
                return             
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE PERS INVÁLIDO (23L12)"
            enviaResposta(resposta, c) 
            conn.close()
            return                                                                         
        if not lancMuldi in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE LANÇAMENTO DE MULDI/MULDI-PREV INVÁLIDA (23M)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        if not compensacao in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE COMPENSAÇÃO NÃO CPRB INVÁLIDA (23N)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if not creditoExt in ["S", "N"]:
            resposta = "99REQUISIÇÃO INVÁLIDA - EXISTÊNCIA DE CRÉDITO EXTEMPORÂNEO PIS/COFINS/IPI INVÁLIDA (23O)"
            enviaResposta(resposta, c) 
            conn.close()
            return                                                                                                                        
        comando = "Select * from Resultados Where TDPF=%s"
        cursor.execute(comando, (chaveTdpf,))
        row = cursor.fetchone() 
        try:
            if row!=None:
                comando = """Update Resultados Set Arrolamentos=%s, MedCautelar=%s, RepPenais=%s, Inaptidoes=%s, Baixas=%s, ExcSimples=%s, 
                             SujPassivos=%s, DigVincs=%s, Situacao11=%s, Interposicao=%s, Situacao15=%s, EstabPrev1=%s, EstabPrev2=%s, Segurados=%s,
                             Prestadores=%s, Tomadores=%s, QtdePER=%s, LancMuldi=%s, Compensacao=%s, CreditoExt=%s, Data=%s, CPF=%s Where TDPF=%s"""
                msg = "ATUALIZAÇÃO"
            else:
                comando = """Insert Into Resultados (Arrolamentos, MedCautelar, RepPenais, Inaptidoes, Baixas, ExcSimples, SujPassivos, DigVincs, 
                             Situacao11, Interposicao, Situacao15, EstabPrev1, EstabPrev2, Segurados, Prestadores, Tomadores, QtdePER, LancMuldi, Compensacao,
                             CreditoExt, Data, CPF, TDPF) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                msg = "INCLUSÃO"
            cursor.execute(comando, (arrolamentos, medCautelar, rffps, inaptidoes, baixas, excSimples, sujPassivos, dvs, situacao11, interposicao, situacao15,
                                     estabPrev1, estabPrev2, segurados, prestadores, tomadores, qtdePER, lancMuldi, compensacao, creditoExt, datetime.now().date(), cpf, chaveTdpf))
            conn.commit()
            resposta = "23SREGISTRO EFETIVADO COM SUCESSO - "+msg
        except:
            conn.rollback()
            resposta = "23NERRO NA "+msg+" DO REGISTRO"
        enviaResposta(resposta, c) 
        conn.close()
        return 

    if codigo==24: #recupera parâmetros de pontuação do TDPF informados pelo usuário, calcula e informa a quantidade de PONTOS dele; ou recupera apenas os parâmetros internos utilizados para cálculo
        if len(msgRecebida)!=(29+tamChave) and len(msgRecebida)!=(32+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (24A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        pediuParametros = False
        if len(msgRecebida)==(32+tamChave):
            if msgRecebida[-3:]!="PAR": #deveria ter solicitado os parâmetros internos
                resposta = "99REQUISIÇÃO INVÁLIDA (24A1)"
                enviaResposta(resposta, c) 
                conn.close()
                return  
            pediuParametros = True 
        resposta = consultaPontuacao(cursor, chaveTdpf, tipoOrgaoUsuario, pediuParametros)             
        if resposta==None:
            resposta = "99REQUISIÇÃO INVÁLIDA - TDPF NÃO FOI LOCALIZADO (24B)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        enviaResposta(resposta, c) 
        conn.close()
        return             

    if codigo==25: #Relação de TDPFs alocados ao CPF ou de que este seja supervisor sem ciência entre XX e YY dias após a emissão
        if len(msgRecebida)!=(17+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (25A)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        inicial = msgRecebida[-4:-2]
        final = msgRecebida[-2:]
        try:
            inicial = int(inicial)
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - QTDE DIAS INICIAL INVÁLIDA (25B)"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        try:
            final = int(final)
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - QTDE DIAS FINAL INVÁLIDA (25C)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if tipoOrgaoUsuario=="L": #considera TDPFs supervisionados e nos quais o fiscal está alocado
            comando = """
                      Select Distinctrow TDPFS.Numero, TDPFS.Nome, TDPFS.Emissao, TDPFS.Vencimento, TDPFS.Codigo
                      From TDPFS, Supervisores
                      Where (Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo and Supervisores.Fiscal=%s) 
                      and TDPFS.Emissao<=cast((now() - interval %s day) as date) and TDPFS.Emissao>=cast((now() - interval %s day) as date) 
                      and TDPFS.Encerramento Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)
                      Union
                      Select Distinctrow TDPFS.Numero, TDPFS.Nome, TDPFS.Emissao, TDPFS.Vencimento, TDPFS.Codigo
                      From TDPFS, Alocacoes              
                      Where (Alocacoes.Fiscal=%s and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo)
                      and TDPFS.Emissao<=cast((now() - interval %s day) as date) and TDPFS.Emissao>=cast((now() - interval %s day) as date) 
                      and TDPFS.Encerramento Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)                
                      Order By Numero
                      """                                                       
            cursor.execute(comando, (chaveFiscal, inicial, final, chaveFiscal, inicial, final))
        elif tipoOrgaoUsuario=="R": #considera TDPFs do órgão e nos quais o fiscal está alocado, se ele estiver na base de fiscais
            comando = """
                      Select Distinctrow TDPFS.Numero, TDPFS.Nome, TDPFS.Emissao, TDPFS.Vencimento, TDPFS.Codigo
                      From TDPFS
                      Where TDPFS.Emissao<=cast((now() - interval %s day) as date) and TDPFS.Emissao>=cast((now() - interval %s day) as date) 
                      and TDPFS.Encerramento Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)
                      and TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) """

            if chaveFiscal==0 or chaveFiscal==None:
                cursor.execute(comando, (inicial, final, orgaoUsuario))  
            else:
                comando = comando + """Union
                                       Select Distinctrow TDPFS.Numero, TDPFS.Nome, TDPFS.Emissao, TDPFS.Vencimento, TDPFS.Codigo
                                       From TDPFS, Alocacoes              
                                       Where (Alocacoes.Fiscal=%s and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo)
                                       and TDPFS.Emissao<=cast((now() - interval %s day) as date) and TDPFS.Emissao>=cast((now() - interval %s day) as date) 
                                       and TDPFS.Encerramento Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)                
                                       Order By Numero                    
                                    """ 
                cursor.execute(comando, (inicial, final, orgaoUsuario, chaveFiscal, inicial, final))
        else: #órgão nacional - seleciona todos os TDPFs
            comando = """
                      Select Distinctrow TDPFS.Numero, TDPFS.Nome, TDPFS.Emissao, TDPFS.Vencimento, TDPFS.Codigo
                      From TDPFS
                      Where TDPFS.Emissao<=cast((now() - interval %s day) as date) and TDPFS.Emissao>=cast((now() - interval %s day) as date) 
                      and TDPFS.Encerramento Is Null and TDPFS.Codigo not in (Select TDPF from Ciencias Where Data Is Not Null)                   
                      """ 
            cursor.execute(comando, (inicial, final))                       
        rows = cursor.fetchall()
        tam = len(rows)
        if tam==0:
            resposta = "2500"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if tam>=100: #limite de 99 tdpfs
            nn = "99"
            tam = 99
        else:
            nn = str(tam).rjust(2,"0")          
        registro = "" 
        resposta = "25"+nn
        i = 0
        total = 0            
        for row in rows:
            tdpf = row[0]
            nome = row[1]
            emissao = row[2]
            if emissao==None:
                emissao = "00/00/0000"
            else:
                emissao = emissao.strftime("%d/%m/%Y")
            vencimento = row[3]
            if vencimento==None:
                vencimento = "00/00/0000"
            else:
                vencimento = vencimento.strftime("%d/%m/%Y")            
            if nome==None or nome=="":
                nome = "ND"  
            nome = nome[:tamNome].ljust(tamNome) 
            #obtem o fiscal há mais tempo alocado ao TDPF
            consulta = "Select Fiscais.Nome From Fiscais, Alocacoes Where Alocacoes.TDPF=%s and Alocacoes.Desalocacao Is Null and Alocacoes.Fiscal=Fiscais.Codigo Order By Alocacoes.Alocacao"
            cursor.execute(consulta, (row[4],)) #passa como parâmetro a chave primária da tabela TDPFS
            fiscaisRow = cursor.fetchone()
            nomeFiscal = None
            if fiscaisRow:
                nomeFiscal = fiscaisRow[0]
            if nomeFiscal==None:
                nomeFiscal = "ND"
            else:
                nomeFiscal = nomeFiscal.split()[0][:20] #manda o primeiro nome do fiscal tb (limitado a 20 caracteres)
            nomeFiscal = nomeFiscal.ljust(20) 
            registro = registro + tdpf + nome + emissao + vencimento + nomeFiscal
            i+=1
            total+=1
            if i==5 or total==tam: #de cinco em cinco ou no último registro, enviamos
                enviaRespostaSemFechar(resposta+registro, c)
                resposta = "25"
                registro = ""
                i = 0
                if total==tam:
                    c.close()
                    return #percorreu os registros ou 99 deles, que é o limite
                if total<tam: #ainda não chegou ao final - aguardamos a requisição da continuação
                    try:
                        mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao
                        requisicao = descriptografa(mensagemRec, addr, c)
                        if requisicao!="2512345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (25D)"
                            enviaResposta(resposta, c) 
                            conn.close()
                            return
                    except:
                        c.close()
                        conn.close()
                        logging.info("Erro de time out 25 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                        return 

    if codigo==27: #Supervisor informa trimestre previsto para encerramento (meta) do TDPF
        trimestreDict = {1:"1", 2:"1", 3:"1", 4:"2", 5:"2", 6:"2", 7:"3", 8:"3", 9:"3", 10:"4", 11:"4", 12:"4"}
        if len(msgRecebida)!=(35+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (27A)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        trimestre = msgRecebida[-6:]
        ano = trimestre[:4]
        trimDigit = trimestre[-1:]
        if not ano.isdigit() or trimestre[4:5]!="/" or not trimDigit.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE INVÁLIDO (27B)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if trimDigit=='0' or trimDigit>'4':      
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE INVÁLIDO (27C)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        trimAtual = str(datetime.now().year)+"/"+trimestreDict[datetime.now().month]           
        if trimAtual>trimestre:
            resposta = "27NTRIMESTRE INFORMADO NÃO PODE SER ANTERIOR AO ATUAL"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        #lá em cima, na verificação comum a diversas funcionalidades, já foi verificado o TDPF, se está em andamento e se o CPF é do supervisor
        comando = "Update TDPFS Set TrimestrePrevisto=%s Where TDPFS.Codigo=%s"
        try:
            cursor.execute(comando, (trimestre, chaveTdpf))      
            conn.commit()
            resposta = "STRIMESTRE ATUALIZADO COM SUCESSO"
        except:
            conn.rollback()
            resposta = "NNÃO FOI POSSÍVEL ATUALIZAR A TABELA DE TDPFS"
        resposta = "27"+resposta
        enviaResposta(resposta, c) 
        conn.close()
        return        

    if codigo==28: #envia lista de fiscais alocados a TDPF (nome fiscalizado, data de encerramento, alocacação, desalocação, horas e nome do fiscal)
        if len(msgRecebida)!=(29+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (28A)"
            enviaResposta(resposta, c) 
            conn.close()
            return            
        comando = """Select TDPFS.Nome, TDPFS.Encerramento, Alocacoes.Alocacao, Alocacoes.Desalocacao, Alocacoes.Horas, Fiscais.Nome, Fiscais.CPF
                     From TDPFS, Alocacoes, Fiscais
                     Where Alocacoes.TDPF=TDPFS.Codigo and TDPFS.Codigo=%s and Alocacoes.Fiscal=Fiscais.Codigo"""
        cursor.execute(comando, (chaveTdpf,))
        rows = cursor.fetchall()
        if not rows or len(rows)==0:
            comando = "Select TDPFS.Nome, TDPFS.Encerramento from TDPFS Where TDPFS.Codigo=%s"
            cursor.execute(comando, (chaveTdpf,))
            row = cursor.fetchone()
            if row[1]==None:
                data = "00/00/0000"
            else:
                data = row[1].strftime("%d/%m/%Y")
            nome = row[0][:100].ljust(100)
            resposta = "28S"+nome+data+"00"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        row = rows[0]
        if row[1]==None:
            data = "00/00/0000"
        else:
            data = row[1].strftime("%d/%m/%Y")
        nome = row[0][:100].ljust(100)   
        nFiscais = len(rows)
        if nFiscais>=100:
            nFiscais = 99
        nFiscais = str(nFiscais).rjust(2,"0")  
        respostaInicio = "28S"+nome+data+nFiscais     
        registro = "" 
        i = 0                  
        for row in rows:
            alocacao = row[2]
            if alocacao!=None:
                alocacao = alocacao.strftime("%d/%m/%Y")
            else:
                alocacao = "00/00/0000"
            desalocacao = row[3]
            horas = row[4]
            if desalocacao==None:
                desalocacao = "00/00/0000"
            else:
                desalocacao = desalocacao.strftime("%d/%m/%Y")
            if horas==None:
                horas = 0
            horas = str(horas).rjust(4, "0")
            nomeFiscal = row[5][:100]
            nomeFiscal = nomeFiscal.ljust(100)
            cpfFiscal = row[6]
            registro = registro + cpfFiscal + nomeFiscal + alocacao + desalocacao + horas
            i+=1
            if i%10==0 or i==len(rows): #de 10 em 10 registros ou no último enviamos
                resposta = respostaInicio + registro
                registro = ""
                if i==len(rows):
                    enviaResposta(resposta, c) 
                    conn.close()
                    return  
                else:
                    enviaRespostaSemFechar(resposta, c)
                    respostaInicio = "28"    
                    try:
                        mensagemRec = c.recv(512)
                        requisicao = descriptografa(mensagemRec, addr, c)
                        if requisicao!="2812345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (28B)"
                            enviaResposta(resposta, c) 
                            conn.close()
                            return
                    except:
                        c.close()
                        conn.close()
                        logging.info("Erro de time out 28 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                        return 

    if codigo==29: #relaciona TDPFs encerrados em um período ou com previsão de encerramento nele sob supervisão do CPF
        mesTrimIni = {"1": "01", "2": "04", "3": "07", "4": "10"}
        mesTrimFim = {"1": "03", "2": "06", "3": "09", "4": "12"}
        qtdeRegistros = 50 #qtde de registros de tdpfs que enviamos por vez (se mudar aqui, tem que alterar o script)
        if len(msgRecebida)!=(25+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (29A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        trimInicial = msgRecebida[-12:-6]
        trimFinal = msgRecebida[-6:]
        anoInicial = trimInicial[:4]
        anoFinal = trimFinal[:4]
        trimIni = trimInicial[5:]
        trimFim = trimFinal[5:]
        if not anoInicial.isdigit() or not anoFinal.isdigit() or trimInicial[4:5]!="/" or trimFinal[4:5]!="/" or not trimIni.isdigit() or not trimFim.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE INVÁLIDO (29B)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if trimInicial>trimFinal:           
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE INICIAL POSTERIOR AO FINAL (29C)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if anoInicial<"2021":
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE INICIAL NÃO PODE SER ANTERIOR A 2021 (29D)"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        if trimFim>"4":
            resposta = "99REQUISIÇÃO INVÁLIDA - TRIMESTRE NÃO PODE SER SUPERIOR A 4 (29E)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        mesIni = mesTrimIni[trimIni]
        mesFim = mesTrimFim[trimFim]
        dataInicial = datetime.strptime("01/"+mesIni+"/"+anoInicial, "%d/%m/%Y")
        dataFinal = datetime.strptime(str(calendar.monthrange(int(anoFinal), int(mesFim))[1])+"/"+mesFim+"/"+anoFinal, "%d/%m/%Y")
        if dataFinal.date().year>datetime.now().date().year:
            resposta = "99REQUISIÇÃO INVÁLIDA -  ANO FINAL NÃO PODE SER FUTURO (29F)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if tipoOrgaoUsuario=="L":           
            comando = """Select TDPFS.Codigo, TDPFS.Numero, TDPFS.Encerramento, TDPFS.TrimestrePrevisto, TDPFS.Grupo, TDPFS.Emissao, Fiscais.Nome, Alocacoes.Horas
                        From TDPFS, Supervisores, Fiscais, Alocacoes
                        Where Supervisores.Fiscal=%s and Supervisores.Equipe=TDPFS.Grupo and Supervisores.Fim Is Null and Alocacoes.TDPF=TDPFS.Codigo and Alocacoes.Fiscal=Fiscais.Codigo
                        and ((Encerramento Is Null and TrimestrePrevisto>=%s and TrimestrePrevisto<=%s) or (Encerramento Is Not Null and Encerramento>=%s and Encerramento<=%s))
                        Order by TDPFS.Grupo, TDPFS.Numero"""
            cursor.execute(comando, (chaveFiscal, trimInicial, trimFinal, dataInicial, dataFinal))
        elif tipoOrgaoUsuario=="R":
            comando = """Select TDPFS.Codigo, TDPFS.Numero, TDPFS.Encerramento, TDPFS.TrimestrePrevisto, TDPFS.Grupo, TDPFS.Emissao, Fiscais.Nome, Alocacoes.Horas
                        From TDPFS, Fiscais, Alocacoes
                        Where TDPFS.Grupo in (Select Equipe from Jurisdicao Where Orgao=%s) and Alocacoes.TDPF=TDPFS.Codigo and Alocacoes.Fiscal=Fiscais.Codigo
                        and ((Encerramento Is Null and TrimestrePrevisto>=%s and TrimestrePrevisto<=%s) or (Encerramento Is Not Null and Encerramento>=%s and Encerramento<=%s))
                        Order by TDPFS.Grupo, TDPFS.Numero"""
            cursor.execute(comando, (orgaoUsuario, trimInicial, trimFinal, dataInicial, dataFinal))  
        else: #órgão nacional
            comando = """Select TDPFS.Codigo, TDPFS.Numero, TDPFS.Encerramento, TDPFS.TrimestrePrevisto, TDPFS.Grupo, TDPFS.Emissao, Fiscais.Nome, Alocacoes.Horas
                        From TDPFS, Fiscais, Alocacoes
                        Where ((Encerramento Is Null and TrimestrePrevisto>=%s and TrimestrePrevisto<=%s) or 
                        (Encerramento Is Not Null and Encerramento>=%s and Encerramento<=%s)) and Alocacoes.TDPF=TDPFS.Codigo and Alocacoes.Fiscal=Fiscais.Codigo
                        Order by TDPFS.Grupo, TDPFS.Numero"""
            cursor.execute(comando, (trimInicial, trimFinal, dataInicial, dataFinal))                               
        rows = cursor.fetchall()
        if not rows or len(rows)==0:
            resposta = "29"+"000"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        i = 0
        tam = len(rows)
        if tam>999:
            tam = 999
        #print("Nº TDPFs supervisionados:  ",tam)
        respostaIn = "29"+str(tam).rjust(3,"0")
        registro = ""
        for row in rows:
            chaveTdpf = row[0]
            tdpf = row[1]
            encerramento = dataTexto(row[2])
            trimestre = row[3]
            grupo = row[4].ljust(25)
            if trimestre==None:
                trimestre = "0000/0" 
            emissao = dataTexto(row[5]) 
            nomeFiscal = row[6][:100].ljust(100)
            horas = row[7]
            if horas==None:
                horas = 0
            horas = str(horas).rjust(4, "0")
            consulta = "Select Data from Ciencias Where TDPF=%s Order By Data"
            cursor.execute(consulta, (chaveTdpf,))
            rowCiencia = cursor.fetchone()
            if rowCiencia==None:
                primCiencia = None
            else:
                primCiencia = rowCiencia[0]
            primCiencia = dataTexto(primCiencia)
            i+=1
            retorno = consultaPontuacao(cursor, chaveTdpf, tipoOrgaoUsuario, False, True)
            if retorno==None:
                pontos = "0000"
            else:
                pontos = retorno[4:8]
            registro = registro + tdpf + encerramento + trimestre + grupo + emissao + primCiencia + pontos + nomeFiscal + horas         
            if i%qtdeRegistros==0 or i==tam:
                resposta = respostaIn + registro
                registro = ""
                if i==tam:
                    enviaResposta(resposta, c) 
                    conn.close()
                    return 
                enviaRespostaSemFechar(resposta, c)
                respostaIn = "29"  
                try:
                    mensagemRec = c.recv(512)
                    requisicao = descriptografa(mensagemRec, addr, c)
                    if requisicao!="2912345678909":
                        resposta = "99REQUISIÇÃO INVÁLIDA (29G)"
                        enviaResposta(resposta, c) 
                        conn.close()
                        return
                except:
                    c.close()
                    conn.close()
                    logging.info("Erro de time out 29 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                    return

    if codigo==32: #envia mensagens da Cofis do dia           
        if len(msgRecebida)!=(13+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (32A)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        comando = "Select Mensagem from MensagensCofis Where Data=%s"
        cursor.execute(comando, (datetime.now().date(),))
        rows = cursor.fetchall()
        if len(rows)==0 or rows==None:
            resposta = "32N"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        i = 0         
        mensagens = ""
        for row in rows:
            i+=1
            mensagens = mensagens + row[0].ljust(100)
            if i==9:
                break
        resposta = "32S"+str(i)+mensagens
        enviaResposta(resposta, c) 
        conn.close()
        return         

    if codigo==33: #inclui prorrogação de um TDPF
        requisicao = ""
        try: #aguarda o restante da mensagem
            mensagemRec = c.recv(2048)
            restante = descriptografa(mensagemRec, addr, c)   
        except:
            c.close()
            conn.close()
            logging.info("Erro de time out 33 - cliente não mandou o complemento da mensagem no prazo. Abandonando operação.")
            return
        if restante[:2]!="33" or restante[2:13]!=cpf:
            resposta = "99REQUISIÇÃO COMPLEMENTAR INVÁLIDA (33A)"
            enviaResposta(resposta, c) 
            conn.close()
            return     
        enviaRespostaSemFechar("33", c) #indicador para enviar os fundamentos            
        try: #aguarda o restante da mensagem
            fundamentos = c.recv(8192).decode("utf-8")  #não há criptografia
            if fundamentos[:2]!="33" or fundamentos[2:13]!=cpf:
                resposta = "99REQUISIÇÃO COMPLEMENTAR INVÁLIDA (33A2)"
                enviaResposta(resposta, c) 
                conn.close()
                return 
            tamanhoFund = int(fundamentos[13:17])
            fundamentos = fundamentos[17:]
            while len(fundamentos)<tamanhoFund:
                fundamentos = fundamentos + c.recv(8192).decode("utf-8")   #não há criptografia
        except:
            c.close()
            conn.close()
            logging.info("Erro de time out 33 - cliente não mandou o complemento da mensagem no prazo. Abandonando operação.")
            return                         
        msgRecebida = msgRecebida + restante[13:]
        if len(msgRecebida)!=(281+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (33A1)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        cpfSuperv = msgRecebida[29+tamChave:40+tamChave]
        assunto = msgRecebida[40+tamChave:140+tamChave].strip()
        numero = msgRecebida[140+tamChave:142+tamChave]
        motivo = msgRecebida[142+tamChave:145+tamChave]
        documento = msgRecebida[145+tamChave:245+tamChave].strip()
        tipo = msgRecebida[245+tamChave:247+tamChave]
        nFiscais = msgRecebida[247+tamChave:248+tamChave]
        if not validaCPF(cpfSuperv):
            resposta = "99REQUISIÇÃO INVÁLIDA - CPF SUPERVISOR INVÁLIDO (33B1)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if  cpfSuperv==cpf:  
            resposta = "99REQUISIÇÃO INVÁLIDA - CPF REQUISITANTE É IGUAL AO DO SUPERVISOR (33B2)"
            enviaResposta(resposta, c) 
            conn.close()
            return                      
        if len(assunto)<10:
            resposta = "99REQUISIÇÃO INVÁLIDA - ASSUNTO (33B)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if not numero.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - NÚMERO INVÁLIDO (33B3)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            numero = int(numero)   
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - NÚMERO INVÁLIDO (33B4)"
            enviaResposta(resposta, c) 
            conn.close()
            return                
        if not motivo.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - MOTIVO INVÁLIDO (33B5)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        try:
            motivo = int(motivo)   
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - MOTIVO INVÁLIDO (33B6)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        if motivo==0:
            resposta = "99REQUISIÇÃO INVÁLIDA - MOTIVO INVÁLIDO (33B7)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if len(documento)<9:
            resposta = "99REQUISIÇÃO INVÁLIDA - DOCUMENTO (33C)"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if not tipo.isdigit():   
            resposta = "99REQUISIÇÃO INVÁLIDA - TIPO (33D)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if not nFiscais.isdigit():   
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE FISCAIS (33E)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        nFiscais = int(nFiscais)
        if nFiscais>4:
            resposta = "99REQUISIÇÃO INVÁLIDA - Nº DE FISCAIS EXCEDIDO (33E1)"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        fundamentos = fundamentos.strip()
        tamFund = len(eliminaTags(fundamentos))
        if tamFund<50 or tamFund>5000:
            resposta = "99REQUISIÇÃO INVÁLIDA - TAMANHO INVÁLIDO DA FUNDAMENTAÇÃO (33K)"
            enviaResposta(resposta, c) 
            conn.close()
            return                       
        fiscais = []
        if nFiscais>1:        
            regFiscais = msgRecebida[-33:]           
            for i in range(nFiscais-1):
                cpfFiscal = regFiscais[i*11:(i*11)+11]
                if cpfFiscal==cpfSuperv:
                    resposta = "99REQUISIÇÃO INVÁLIDA - FISCAL ALOCADO NÃO PODE SER SUPERVISOR (33B3)"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return                     
                if not cpfFiscal in fiscais:
                    fiscais.append(cpfFiscal)      
        consulta = "Select Codigo from Prorrogacoes Where TDPF=%s and Data=%s" 
        cursor.execute(consulta, (chaveTdpf, datetime.now().date())) 
        row = cursor.fetchone()
        if row:
            if row[0]!=None:
                resposta = "33J"
                enviaResposta(resposta, c) 
                conn.close()
                return
        comando = """
                  Select Prorrogacoes.Codigo from Prorrogacoes, AssinaturaFiscal Where TDPF=%s and AssinaturaFiscal.Prorrogacao=Prorrogacoes.Codigo and 
                  (Prorrogacoes.DataAssinatura Is Null or AssinaturaFiscal.DataAssinatura Is Null)
                  """
        cursor.execute(comando, (chaveTdpf,))
        row = cursor.fetchone()
        if row: #há assinatura pendente em prorrogação incluída
            resposta = "33J"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        #verificamos se o cpfSuperv (supervisor titular ou substituto) é supervisor titular ou pelo menos tem algum TDPF alocado na mesma equipe (substituto)
        comando = """Select Fiscais.Codigo, Fiscais.CPF, TDPFS.Grupo From Fiscais, TDPFS, Supervisores 
                      Where TDPFS.Codigo=%s and TDPFS.Grupo=Supervisores.Equipe and Supervisores.Fim Is Null and Supervisores.Fiscal=Fiscais.Codigo"""
        cursor.execute(comando, (chaveTdpf,))
        row = cursor.fetchone()
        supervisor = None
        if row:
            if row[0]!=None:
                supervisor = row[0]
        if supervisor==None:
            resposta = "33C"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        if row[1]!=cpfSuperv: #se o supervisor da equipe não for o titular, vemos se o informado está alocado a pelo menos um TDPF da mesma equipe
            comando = """Select * from TDPFS, Fiscais, Alocacoes Where TDPFS.Grupo=%s and TDPFS.Codigo=Alocacoes.TDPF and
                         Alocacoes.Desalocacao Is Null and Alocacoes.Fiscal=Fiscais.Codigo and Fiscais.CPF=%s"""   
            cursor.execute(comando, (row[2], cpfSuperv))  
            row = cursor.fetchone() #tem que haver pelo menos um TDPF
            if not row:
                resposta = "33C"
                enviaResposta(resposta, c) 
                conn.close()
                return   
        comando = "Select Codigo from Prorrogacoes Where TDPF=%s Order by Numero DESC"                  
        cursor.execute(comando, (chaveTdpf, ))
        row = cursor.fetchone()
        if row:
            if row[0]>=numero: #número da prorrogação que recebemos deve ser maior do que o último registrado para o TDPF
                resposta = "33X"
                enviaResposta(resposta, c) 
                conn.close()
                return 
        comando = "Insert Into Prorrogacoes (TDPF, Assunto, Documento, Tipo, Data, Supervisor, Fundamentos, Numero, Motivo) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(comando, (chaveTdpf, assunto, documento, tipo, datetime.now().date(), supervisor, fundamentos, numero, motivo))
        cursor.execute(consulta, (chaveTdpf, datetime.now().date())) 
        row = cursor.fetchone()
        bErro = False
        if row==None:
            bErro = True
        elif len(row)==0:
            bErro = True
        if bErro:
            conn.rollback()
            resposta = "33F"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        chaveProrrogacao = row[0]   
        consulta = "Select Fiscais.Codigo From Fiscais, Alocacoes Where Fiscais.CPF=%s and Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.TDPF=%s and Alocacoes.Desalocacao Is Null"
        listaInclusao = [(chaveProrrogacao, chaveFiscal, datetime.now())]
        bErro = False
        for cpfFiscal in fiscais:
            cursor.execute(consulta, (cpfFiscal, chaveTdpf))
            rowFiscal = cursor.fetchone()
            if rowFiscal==None:
                bErro = True
            elif len(rowFiscal)==0:
                bErro = True
            if bErro:
                conn.rollback()
                resposta = "33O"
                enviaResposta(resposta, c) 
                conn.close()
                return 
            listaInclusao.append((chaveProrrogacao, rowFiscal[0], None))         
        comando = "Insert Into AssinaturaFiscal (Prorrogacao, Fiscal, DataAssinatura) Values (%s, %s, %s)"
        cursor.executemany(comando, listaInclusao)
        try:
            conn.commit()
            resposta = "33S"
        except:
            conn.rollback()
            resposta = "33F"
        enviaResposta(resposta, c) 
        conn.close()
        return 

    if codigo==34: #consulta prorrogações de um TDPF
        if len(msgRecebida)!=(29+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (34A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        comando ="""Select Prorrogacoes.Codigo, Assunto, Documento, Tipo, Data, Fiscais.CPF, Fiscais.Nome, DataAssinatura, Fundamentos, Prorrogacoes.Numero, RegistroRHAF 
                 from Prorrogacoes, Fiscais Where TDPF=%s and Supervisor=Fiscais.Codigo Order by Data"""    
        cursor.execute(comando, (chaveTdpf,))           
        prorrogacoes = cursor.fetchall()
        if prorrogacoes==None:
            resposta = "34S00"
            enviaResposta(resposta, c)
            conn.close()
            return            
        elif len(prorrogacoes)==0:
            resposta = "34S00"
            enviaResposta(resposta, c)
            conn.close()
            return            
        else:
            i = 0
            tam = len(prorrogacoes)
            if tam>99:
                tam = 99
            consulta = "Select Fiscais.CPF, Fiscais.Nome, DataAssinatura from Fiscais, AssinaturaFiscal Where AssinaturaFiscal.Fiscal=Fiscais.Codigo and AssinaturaFiscal.Prorrogacao=%s"
            for prorrogacao in prorrogacoes:
                i+=1
                chaveProrrogacao = prorrogacao[0]
                assunto = prorrogacao[1].ljust(100)
                documento = prorrogacao[2].ljust(100)
                tipo = prorrogacao[3].ljust(2)
                data = dataTexto(prorrogacao[4])
                cpfSupervisor = prorrogacao[5]
                nomeSupervisor = prorrogacao[6][:100].ljust(100)
                assSupervisor = dataTexto(prorrogacao[7])
                fundamentos = prorrogacao[8]
                if fundamentos==None:
                    fundamentos = "" #não deve acontecer, mas ...
                nTermo = prorrogacao[9]
                if nTermo==None:
                    nTermo = 0
                nTermo = str(nTermo).rjust(2, "0") 
                registroRhaf = dataTexto(prorrogacao[10])                   
                cursor.execute(consulta,(chaveProrrogacao,))
                fiscais = cursor.fetchall()
                registro = ""
                j = 0
                for fiscal in fiscais:
                    j+=1
                    registro = registro + fiscal[0] + fiscal[1][:100].ljust(100)+dataTexto(fiscal[2])
                    if j==4:
                        break #no máximo 4 fiscais (não deve ter mais, mas prefiro não arriscar)
                registro = registro.ljust(484)
                registro = assunto+documento+tipo+data+nTermo+cpfSupervisor+nomeSupervisor+assSupervisor+registroRhaf+registro
                if i==1:
                    resposta = "34S"+str(tam).rjust(2, "0") + registro
                else:
                    resposta = "34" + registro
                if i==tam:
                    enviaRespostaSemFechar(resposta, c)
                    if not espera34("1", c, conn, addr):
                        return                       
                    enviaResposta("34"+fundamentos, c)
                    conn.close()
                    return
                else:
                    enviaRespostaSemFechar(resposta, c)
                    if not espera34("2", c, conn, addr):
                        return                    
                    enviaRespostaSemFechar("34"+fundamentos, c)
                    if not espera34("3", c, conn, addr):
                        return                   

    if codigo==35: #exclui prorrogação (deve ser a última; deve estar com assinatura pendente ou cpf do supervisor)
        if len(msgRecebida)!=(39+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (35A)"
            enviaResposta(resposta, c) 
            conn.close()
            return    
        data = msgRecebida[-10:]
        if not isDate(data):    
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA INVÁLIDA (35B)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        data = datetime.strptime(data, "%d/%m/%Y")
        if data.date()>datetime.now().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA FUTURA (35C)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        comando = "Select Codigo, DataAssinatura, Data, Supervisor From Prorrogacoes Where TDPF=%s Order by Data DESC" 
        cursor.execute(comando, (chaveTdpf,))
        row = cursor.fetchone()
        if not row:
            resposta = "35P"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        chaveProrrogacao = row[0] 
        if row[2].date()!=data.date(): #não é a última prorrogação ou não existe a data
            resposta = "35L"
            enviaResposta(resposta, c) 
            conn.close()
            return                                              
        if row[3]!=chaveFiscal and not bSupervisor: #usuário não é supervisor do TDPF incluído na prorrogação
            if row[1]!=None: #só é permitida a exclusão se houver assinatura pendente de alguém (e a do supervisor não está pendente)
                comando = "Select * from AssinaturaFiscal Where Prorrogacao=%s and DataAssinatura Is Null"
                cursor.execute(comando, (chaveProrrogacao,))
                assinaturas = cursor.fetchall()
                if len(assinaturas)==0: #usuário não é supervisor e não há assinaturas pendentes
                    resposta = "35U"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return   
        comando = "Delete From AssinaturaFiscal Where Prorrogacao=%s"
        cursor.execute(comando, (chaveProrrogacao,))
        comando = "Delete From Prorrogacoes Where Codigo=%s"
        cursor.execute(comando, (chaveProrrogacao,))
        try:
            conn.commit()
            resposta = "35S"
        except:
            conn.rollback()
            resposta = "35F"
        enviaResposta(resposta, c) 
        conn.close()
        return         

    if codigo==36: #informa assinatura na prorrogação
        if len(msgRecebida)!=(39+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (36A)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        dataDoc = msgRecebida[-10:]
        if not isDate(dataDoc):
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA INVÁLIDA (36B)"
            enviaResposta(resposta, c) 
            conn.close()
            return                     
        dataDoc = datetime.strptime(dataDoc, "%d/%m/%Y")
        if dataDoc.date()>datetime.now().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA FUTURA (36C)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        comando = "Select Codigo, DataAssinatura, Supervisor from Prorrogacoes Where TDPF=%s and Data=%s"
        cursor.execute(comando, (chaveTdpf, dataDoc))
        row = cursor.fetchone()
        if not row:
            resposta = "36P"
            enviaResposta(resposta, c) 
            conn.close()
            return          
        chaveProrrogacao = row[0]
        assSupervisor = row[1]
        supervisor = row[2]
        if supervisor==chaveFiscal: #é o supervisor (efetivo ou substituto) - temos que ver se ele é o último a assinar e se sua assinatura está pendente
            comando = "Select * from AssinaturaFiscal Where DataAssinatura Is Null and Prorrogacao=%s"
            cursor.execute(comando, (chaveProrrogacao,))
            rowRestantes = cursor.fetchall()
            if rowRestantes:
                if len(rowRestantes)>0: #há assinaturas pendentes de outros fiscais
                    resposta = "36P"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return 
            comando = "Update Prorrogacoes Set DataAssinatura=%s Where Codigo=%s"
            cursor.execute(comando, (datetime.now(), chaveProrrogacao))
        else:
            comando = "Select Codigo, DataAssinatura from AssinaturaFiscal Where Fiscal=%s and Prorrogacao=%s"
            cursor.execute(comando, (chaveFiscal, chaveProrrogacao))   
            row = cursor.fetchone()
            if row:
                chaveRegistro = row[0]
                if row[1]!=None: #fiscal já assinou
                    resposta = "36P"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return
                comando = "Update AssinaturaFiscal Set DataAssinatura=%s Where Codigo=%s"  
                cursor.execute(comando, (datetime.now(), chaveRegistro))
            else:
                resposta = "36P"
                enviaResposta(resposta, c) 
                conn.close()
                return
        try:
            conn.commit()
            resposta = "36S"
        except:
            conn.rollback()
            resposta = "36F"
        enviaResposta(resposta, c) 
        conn.close()
        return  

    if codigo==37: #retorna lista de TDPFs pendentes de assinatura pelo cpf do usuário
        #assinatura do supervisor deve ser a última
        comando = """
                  Select TDPFS.Numero, Data, Prorrogacoes.Numero from TDPFS, Prorrogacoes, AssinaturaFiscal
                  Where TDPFS.Codigo=Prorrogacoes.TDPF and Prorrogacoes.Supervisor=%s and Prorrogacoes.DataAssinatura Is Null and
                  AssinaturaFiscal.Prorrogacao=Prorrogacoes.Codigo and AssinaturaFiscal.DataAssinatura Is Not Null
                  Union
                  Select TDPFS.Numero, Data, Prorrogacoes.Numero from TDPFS, Prorrogacoes, AssinaturaFiscal
                  Where TDPFS.Codigo=Prorrogacoes.TDPF and AssinaturaFiscal.Prorrogacao=Prorrogacoes.Codigo and
                  AssinaturaFiscal.Fiscal=%s and AssinaturaFiscal.DataAssinatura Is Null
                  """                 
        listaTdpfs = []
        cursor.execute(comando, (chaveFiscal, chaveFiscal))   
        rows = cursor.fetchall()
        for row in rows:
            if not [row[0], row[1]] in listaTdpfs:
                listaTdpfs.append([row[0], row[1], row[2]]) 
        tam = len(listaTdpfs)
        if tam==0:
            resposta = "3700"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if tam>99:
            tam = 99
        registro = ""
        for i in range(tam):
            registro = registro + listaTdpfs[i][0]+listaTdpfs[i][1].strftime("%d/%m/%Y")+str(listaTdpfs[i][2]).rjust(2, "0")
        resposta = "37"+str(tam).rjust(2,"0")+registro
        enviaResposta(resposta, c) 
        conn.close()
        return 

    if codigo==38: #solicita próximo número de prorrogação para numerar o termo
        consulta = "Select Numero from Prorrogacoes Where TDPF=%s order by Numero DESC"        
        cursor.execute(consulta, (chaveTdpf,))
        row = cursor.fetchone()
        if not row:
            resposta = "38S01"
        else:
            resposta = "38S"+str(row[0]+1).rjust(2, "0")
        enviaResposta(resposta, c) 
        conn.close()
        return  

    if codigo==39: #solicita prorrogações pendentes de assinatura no RHAF (todos assinaram a prorrogação) - usuário deve ser fiscal alocado ou supervisor
        consulta = """
                    Select Distinctrow TDPFS.Numero, TDPFS.Nome, Prorrogacoes.Data, Prorrogacoes.Numero, Motivo, Fundamentos
                    from Prorrogacoes, TDPFS, AssinaturaFiscal, Fiscais, Alocacoes, Supervisores
                    Where Fiscais.CPF=%s and ((Alocacoes.Fiscal=Fiscais.Codigo and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo) or
                    (Supervisores.Fiscal=Fiscais.Codigo and Supervisores.Fim Is Null and Supervisores.Equipe=TDPFS.Grupo)) and TDPFS.Codigo=Prorrogacoes.TDPF and
                    AssinaturaFiscal.Prorrogacao=Prorrogacoes.Codigo and Prorrogacoes.DataAssinatura Is Not Null and RegistroRHAF Is Null and
                    AssinaturaFiscal.DataAssinatura Is Not Null Order By Prorrogacoes.Data, TDPFS.Numero
                   """        
        cursor.execute(consulta, (cpf,))
        resposta = ""
        rows = cursor.fetchall()
        if not rows:
            resposta = "39N"  
        elif len(rows)==0:
            resposta = "39N" 
        else:
            tam = len(rows) 
        if resposta=="39N":
            enviaResposta(resposta, c) 
            conn.close()
            return   
        if tam>99:
            tam = 99 
        resposta = "39S"+str(tam).rjust(2,"0")
        i = 0
        for row in rows:
            tdpf = row[0]
            nome = row[1]
            if nome==None:
                nome = ""
            nome = nome[:100].ljust(100)
            data = dataTexto(row[2])
            numero = row[3]
            motivo = row[4]
            if motivo==None:
                motivo = 0
            fundamentos = row[5]
            if fundamentos==None:
                fundamentos = ""
            resposta = resposta + tdpf + nome + data + str(numero).rjust(2, "0")+str(motivo).rjust(3, "0")
            enviaRespostaSemFechar(resposta, c)
            resposta = "39"             
            try:
                mensagemRec = c.recv(256)
                requisicao = descriptografa(mensagemRec, addr, c)
                if requisicao!="3912345678909":
                    resposta = "99REQUISIÇÃO INVÁLIDA (39G)"
                    enviaResposta(resposta, c) 
                    conn.close()
                    return
                if i==tam:
                    enviaResposta("39"+fundamentos.strip(), c)
                    conn.close()
                    return                      
                else:
                    enviaRespostaSemFechar("39"+fundamentos.strip(), c)
                    try:
                        mensagemRec = c.recv(256)
                        requisicao = descriptografa(mensagemRec, addr, c)
                        if requisicao!="3912345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (39H)"
                            enviaResposta(resposta, c) 
                            conn.close()
                            return     
                    except:
                        c.close()
                        conn.close()
                        logging.info("Erro de time out 39A - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                        return                                           
            except:
                c.close()
                conn.close()
                logging.info("Erro de time out 39B - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                return  
        return   

    if codigo==40: #informa registro no RHAF relativamente a uma prorrogação
        if len(msgRecebida)!=(39+tamChave) and len(msgRecebida)!=(40+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (40A)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        bExclui = False
        if len(msgRecebida)==(40+tamChave):
            if msgRecebida[-1:]!="E":
                resposta = "99REQUISIÇÃO INVÁLIDA (40A1)"
                enviaResposta(resposta, c) 
                conn.close()
                return 
            msgRecebida = msgRecebida[:39+tamChave]
            bExclui = True
        dataDoc = msgRecebida[-10:]
        if not isDate(dataDoc):
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA INVÁLIDA (40B)"
            enviaResposta(resposta, c) 
            conn.close()
            return                     
        dataDoc = datetime.strptime(dataDoc, "%d/%m/%Y")
        if dataDoc.date()>datetime.now().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA FUTURA (40C)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        comando = "Select Codigo, DataAssinatura from Prorrogacoes Where TDPF=%s and Data=%s" #como supervisor assina por último, sua assinatura não pode estar nula
        cursor.execute(comando, (chaveTdpf, dataDoc))
        row = cursor.fetchone()
        if not row:
            resposta = "40P"
            enviaResposta(resposta, c) 
            conn.close()
            return          
        chaveProrrogacao = row[0]
        if not bExclui: #inclui o registro no RHAF para a prorrogação
            comando = "Select * from AssinaturaFiscal Where Fiscal=%s and Prorrogacao=%s"
            cursor.execute(comando, (chaveFiscal, chaveProrrogacao)) #o fiscal atual deve constar da prorrogação
            rowFiscal = cursor.fetchone()
            if rowFiscal==None:
                resposta = "40N"
                enviaResposta(resposta, c) 
                conn.close()
                return
            assSupervisor = row[1]
            if assSupervisor==None: #supervisor já deve ter assinado (o último a assinar)
                resposta = "40P"
                enviaResposta(resposta, c) 
                conn.close()
                return     
            comando = "Update Prorrogacoes Set RegistroRHAF=%s Where Codigo=%s"
            cursor.execute(comando, (datetime.now(), chaveProrrogacao))            
        else: #devemos excluir o registro no RHAF para a prorrogação
            comando = "Update Prorrogacoes Set RegistroRHAF=Null Where Codigo=%s"
            cursor.execute(comando, (chaveProrrogacao, ))            
        try:
            conn.commit()
            resposta = "40S"
        except:
            conn.rollback()
            resposta = "40F"
        enviaResposta(resposta, c) 
        conn.close()
        return  


    if codigo==60: #Solicita DCCs vinculados aos TDPFs em andamento (somente usuários autorizados - CPF1, CPF2 e CPF3 [variáveis de ambiente] - e demanda mais uma senha)
        if len(msgRecebida)!=(23+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (60A)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        hora = datetime.now().hour
        if 8<hora<10:
            resposta = "60H"
            enviaResposta(resposta, c) 
            conn.close()
            return         
        if not cpf in [CPF1, CPF2, CPF3]:
            resposta = "60N"
            enviaResposta(resposta, c) 
            conn.close()
            return
        senha = msgRecebida[-10:]
        horaSenha = str(int(int(datetime.now().strftime('%H%M'))/10)).rjust(3,"0")
        if senha[-10:-3]!=SENHADCCS or senha[-3:]!=horaSenha or SENHADCCS==None: #senha é DIS@V71 seguida da hora e a dezena dos minutos (ex.: são 11:35, a senha é DIS@V71113)
            resposta = "60N"
            enviaResposta(resposta, c) 
            conn.close()
            return
        comando = "Select DCC from TDPFS Where DCC Is Not Null and DCC<>'' and Encerramento Is Null"
        cursor.execute(comando)
        rows = cursor.fetchall()
        tam = len(rows)
        tamStr = str(tam).rjust(5,"0")
        resposta = "60S"+tamStr
        if tam==0:
            enviaResposta(resposta, c) 
            conn.close()
            return            
        i = 0
        for row in rows:
            resposta = resposta + row[0]
            i+=1
            if i%50==0 or i==tam:
                if i==tam:
                    enviaResposta(resposta, c) 
                    conn.close()
                    return 
                enviaRespostaSemFechar(resposta, c) 
                resposta = "60"  
                try:
                    mensagemRec = c.recv(256)
                    requisicao = descriptografa(mensagemRec, addr, c)
                    if requisicao!="60"+cpf:
                        resposta = "99REQUISIÇÃO INVÁLIDA (60B)"
                        enviaResposta(resposta, c) 
                        conn.close()
                        return
                except:
                    c.close()
                    conn.close()
                    logging.info("Erro de time out 60 - provavelmente cliente não respondeu no prazo. Abandonando operação.")
                    return                            
                   
    return #não chega aqui, mas ...
      
def espera34(n, c, conn, addr):
    try:
        mensagemRec = c.recv(512)
        requisicao = descriptografa(mensagemRec, addr, c)
        if requisicao!="3412345678909":
            resposta = "99REQUISIÇÃO INVÁLIDA (34B)"
            enviaResposta(resposta, c) 
            conn.close()
            return False
        else:
            return True
    except:
        c.close()
        conn.close()
        logging.info("Erro de time out 34 "+n+" - provavelmente cliente não respondeu no prazo. Abandonando operação.")
        return False    

def servidor(): 
    global  threads, s, pubKey

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)          
    logging.info("Socket successfully created")
     
    port = 1352             
      
    # Next bind to the port 
    # we have not typed any ip in the ip field 
    # instead we have inputted an empty string 
    # this makes the server listen to requests  
    # coming from other computers on the network 
    s.bind(('', port))         
    logging.info("socket binded to %s" %(port))
      
    # put the socket into listening mode 
    if ambiente=="TESTE":
        s.listen(5)   
    else:
        s.listen(10)  
    logging.info("socket is listening")
      
    #só fica escutando a rede, pega a mensagem e encaminha para tratamento 
    while True:  
        try:
            c, addr = s.accept()      
            #print(addr)
            #print(type(addr))
            logging.info('Got connection from ' + str(addr))    
            c.settimeout(10)
            try:
                msgRecebida = c.recv(2048)#.decode('utf-8') #recebe a mensagem  (era 1024)
                #logging.info(binascii.hexlify(msgRecebida))
                #logging.info(len(msgRecebida))
    
                threadTrata = threading.Thread(target=trataMsgRecebida, args=(msgRecebida,c, addr[0]))
                threads.append(threadTrata)
                threadTrata.start()   #inicia a thread que vai tratar a requisição       
                #trataMsgRecebida(msgRecebida, c) #monta a resposta e a envia
               
            except:
                c.close()
                logging.info("Time out ", addr)
        except:
            logging.info("Socket que estava 'ouvindo' a rede parou de funcionar. Se fechou o programa, tudo bem. Senão ...")
            return
       
       #c.close() #com as threads, vai ter que fechar lá na função (thread)
       

#encryptor = PKCS1_OAEP.new(pubKey) #<-- lado cliente (abaixo exemplo)
#msgCripto = encryptor.encrypt(str.encode("Testando Cripto 1111111111111 00000000000000 44444444444444444 55555555555555555555555 6666666666666666666666 77777777777777777777777777"))
#decrypted = chavesCripto[0].decrypt(msgCripto).decode("utf-8") #<-- exemplo para o servidor (aqui)
#print(decrypted)
#h = open('mykeyPrivada.pem','rb') <-- antigamente
#privKey = RSA.import_key(h.read()) <-- antigamente
#decryptor = PKCS1_OAEP.new(privKey) <-- antigamente

s = None
interrompe = False
sistema = sys.platform.upper()
if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
    hostSrv = 'localhost'
    dirLog = 'log\\'
else:
    hostSrv = 'mysqlsrv'
    dirLog = '/Log/' 
logging.basicConfig(filename=dirLog+datetime.now().strftime('%Y-%m-%d %H_%M')+' Serv'+sistema+'.log', format='%(asctime)s - %(message)s', level=logging.INFO)    
MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234")
CPF1 = os.getenv("CPF1", None)
SENHADCCS = os.getenv("SENHADCCS", None)
CPF2 = os.getenv("CPF2", None)
CPF3 = os.getenv("CPF3", None)
token = os.getenv("TOKEN", "ERRO")
ambiente = os.getenv("AMBIENTE", "TESTE")

conn = conecta() #testa a conexão com o BD 
if conn!=None:
    conn.close()
    #contém as chaves [0], a função de descriptografia [1] e a data/hora de vencimento [2] para cada segmento IP (resto da divisão da soma das partes do IP por 10 ou 20; 10/20 segmentos)
    chavesCripto = dict()
    print("Gerando chaves criptográficas ...")
    start = time.time()
    inicializaChaves()
    end = time.time()
    print(str(len(chavesCripto))+" chaves geradas em "+str(end - start)[:7]+" segundos.")    
    threads = list() 
    threadServ = threading.Thread(target=servidor, daemon=True) #ativa o servidor
    #threadServ.daemon = True #mata a thread quando sair do programa
    threadServ.start()
    while True:
        sair = input("Digite QUIT quando quiser sair: ")
        if sair:
            if sair.upper().strip()=="QUIT":
                break
    if s!=None: #fecha o socket principal (ouvindo a rede) se estiver aberto
        s.close() #fecha antes de esperar o término das threads para não entrar mais requisições
    i = 0        
    for thread in (threads):
        if thread.is_alive():
            i+=1              #ao interromper o loop, espera as threads que estão tratando requisições
            thread.join()     #terminarem antes de encerrar o programa (fechar conexões)
    logging.info(str(i) + " threads estavam em andamento.")
else:
    print("Não foi possível conectar ao MySQL. Corrija o problema do Banco de Dados e reinicie este serviço.")
    logging.info("Erro ao tentar conectar ao BD - Saindo ...")