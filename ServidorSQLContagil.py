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

def descriptografa(msgCripto):
    global decryptor, chavesCripto
    decrypted = chavesCripto[0].decrypt(msgCripto).decode("utf-8")
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
    cpf = getAlgarismos(cpfPar)
    if len(cpf)!=11:
        return False

    if cpf in [s * 11 for s in [str(n) for n in range(10)]]:
        return False

    calc = lambda t: int(t[1]) * (t[0] + 2)
    d1 = (sum(map(calc, enumerate(reversed(cpf[:-2])))) * 10) % 11
    d2 = (sum(map(calc, enumerate(reversed(cpf[:-1])))) * 10) % 11
    return str(d1) == cpf[-2] and str(d2) == cpf[-1]

#transforma uma data string de dd/mm/yyyy para yyyy/mm/dd para fins de consulta, inclusão ou atualização no BD SQL
#se o BD esperar a data em outro formato, basta alterarmos aqui
def converteAMD(data):
    return data[6:]+"/"+data[3:5]+"/"+data[:2]


def verificaEMail(email): #valida o e-mail se o usuário informou um completo
    regex1 = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    regex2 = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+[.]\w{2,3}$'  

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

def enviaRespostaSemFechar(resposta, c, cont=0):
    #logging.info(resposta)    
    if cont==0:
        resposta = resposta.encode('utf-8')   
    try:    
        ret = c.send(resposta)
        if ret > 0 and ret < len(resposta):
            logging.info("Não enviou tudo")
            return enviaRespostaSemFechar(resposta[ret:], c, 1)
        else:
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

def verificaSupervisao(conn, cpf, tdpf): #verifica se o fiscal (cpf) é supervisor da equipe do fiscal responsável pelo TDPF em andamento - retorna tb o nome do fiscalizado
    cursor = conn.cursor(buffered=True)
    comando = """Select TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao 
                from TDPFS, Supervisores, Fiscais 
                Where TDPFS.Numero=%s and TDPFS.Grupo=Supervisores.Equipe and Fiscais.CPF=%s and Fiscais.Codigo=Supervisores.Fiscal 
                and TDPFS.Encerramento Is Null"""
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

def geraChaves(): #gera a chave (par) do usuário, coloca no dicionário de validades e retorna a chave pública para ser encaminhada a ele
    global chavesCripto
    #import binascii
    keyPair = RSA.generate(2048)
    decryptor = PKCS1_OAEP.new(keyPair)        
    chavesCripto = [decryptor, datetime.now()+timedelta(hours=1)] #validade de 1h para este conjunto de chaves
    pubKey = keyPair.publickey()
    return pubKey
    #print(f"Public key:  (n={hex(pubKey.n)}, e={hex(pubKey.e)})")
    #pubKeyPEM = pubKey.exportKey()   

def estaoChavesValidas():
    global chavesCripto
    if chavesCripto==[]:
        return False #não há chave a ser revalidada
    if chavesCripto[1]<datetime.now():
        return False
    else:
        return True

def trataMsgRecebida(msgRecebida, c): #c é o socket estabelecido com o cliente que será utilizado para a resposta
    global pubKey, chavesCripto
    tamChave = 6 #tamanho da chave do ContÁgil (chave de registro) 
    tamMsg = 100
    tamNome = 100
    try:
        msgOrigem = msgRecebida.decode("utf-8")     
        if len(msgOrigem)==13:
            if msgOrigem[:2]=="00" and validaCPF(msgOrigem[2:]): #está fazendo uma requisição de chave pública (msg sem criptografia)
                if not estaoChavesValidas(): #meio difícil de ocorrer aqui, pq tem lá no servidor, mas ...
                    pubKey = geraChaves() 
                enviaRespostaSemFechar("0000"+chavesCripto[1].strftime("%d/%m/%Y %H:%M:%S"), c)
                try:
                    msg = c.recv(1024).decode("utf-8") #só aguarda um 00
                    if msg=="00":
                        c.sendall(pubKey.export_key()) #duas mensagens na sequência sem aguardar resposta
                except:
                    logging.info("Não enviou o flag para receber a chave "+msgOrigem[2:])
                c.close()
                return
    except:
        pass #não conseguiu decodificar a msgRecebida
                      
    msgRecebida = descriptografa(msgRecebida)
    c.settimeout(10)
    #todas as mensagens tem um código, um cpf, um tdpf para acesso e uma chave deste - total: 39 caracteres
    if len(msgRecebida)<(13+tamChave):
        resposta = "99REQUISIÇÃO INVÁLIDA (A)" #código de erro na mensagem recebida
        enviaResposta(resposta, c)
        return 
    
    codigoStr = msgRecebida[:2]
    cpf = msgRecebida[2:13]
    codigo = int(codigoStr)    
    chaveContagil = msgRecebida[13:(13+tamChave)]      

    logging.info(codigoStr)
    logging.info(cpf)
    logging.info(chaveContagil)     
   
    if not codigoStr.isdigit():
        resposta = "99REQUISIÇÃO INVÁLIDA (B)"
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

        
    if codigo<1 or codigo>20:
        resposta = "99REQUISIÇÃO INVÁLIDA (C)" 
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
    if codigo==1: #status do usuário 
        comando = "Select Codigo, CPF, Adesao, Saida, d1, d2, d3, email, Chave, ValidadeChave, Tentativas from Usuarios Where CPF=%s"
        cursor.execute(comando, (cpf,))
        rows = cursor.fetchall() #pode haver mais de um cadastro por CPF, mas só um ativo
        if len(rows)==0:
            resposta = "0103" #01 - status; 03 - não registrado
            enviaResposta(resposta, c) 
            conn.close()
            return            
        ativo = False
        for row in rows:
            if row[3]==None and row[2]!=None: #tem um registro ativo
                ativo = True
                tentativas = row[10]
                if tentativas==None:
                    tentativas = 0
                chaveBD = row[8]
                if chaveBD==None:
                    chaveBD = -int(chaveContagil)  
                if tentativas>=3:
                    result = "4"
                elif chaveBD==int(chaveContagil):
                    if row[9]==None:
                        result = "2"
                    else:  
                        logging.info(row[9].date())                        
                        if datetime.today().date()<=row[9].date():
                            result = "1" #chave dentro da validade
                            zeraTentativas(row[0], conn)
                        else:
                            result = "2" #chave fora da validade
                else: #chave digitada não confere 
                    result = "3"
                    atualizaTentativas(row[0], tentativas, conn)
                break
        if ativo:
            resposta = "0101"+result #01 - status; 01 - ativo
        else:
            resposta = "0102" #01 - status; 02 - inativo   
        enviaResposta(resposta, c) 
        conn.close()
        return    
    
    
    #validamos a chave do contágil ligada àquele CPF (registro ativo) - serviços de 2 em diante
    comando = "Select Codigo, Chave, ValidadeChave, Tentativas, email, d1, d2, d3 from Usuarios Where CPF=%s and  Saida Is Null and Adesao Is Not Null"            
    cursor.execute(comando, (cpf,))
    row = cursor.fetchone()   
    if not row: #o usuário está inativo
        resposta = "90USUÁRIO NÃO ENCONTRADO OU INATIVO"
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
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL SEM VALIDADE - GERE OUTRA NO TELEGRAM"
        enviaResposta(resposta, c) 
        conn.close()
        return        
    if datetime.today().date()>row[2].date():
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL ESTÁ EXPIRADA - GERE OUTRA NO TELEGRAM"
        enviaResposta(resposta, c)  
        conn.close()
        return    
    if tentativas>=3:
        resposta = "90CHAVE DE ACESSO VIA CONTÁGIL ESTÁ EXPIRADA - TENTATIVAS EXCEDIDAS - GERE OUTRA NO TELEGRAM"
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
    
    #obtemos a chave do fiscal para ser utilizada em todas as requisições    
    comando = "Select Fiscais.Codigo From Fiscais Where Fiscais.CPF=%s"          
    cursor.execute(comando, (cpf,))
    rowFiscal = cursor.fetchone()
    if not rowFiscal:
        resposta = "97CPF DO FISCAL NÃO FOI LOCALIZADO"
        enviaResposta(resposta, c)  
        conn.close()
        return 
    chaveFiscal = rowFiscal[0] #<---

    if codigo in [2, 3, 4, 5, 14, 15, 16, 17, 18, 19, 20]: #verificações COMUNS relativas ao TDPF (TDPF existe, em andamento, cpf está alocado nele)
        if len(msgRecebida)<(29+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (3)"
            enviaResposta(resposta, c)  
            conn.close()
            return                   
        tdpf = msgRecebida[(tamChave+13):(tamChave+29)] #obtemos o TDPF que será utilizado em todas as requisições acima elencadas
        if not tdpf.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA - TDPF DEVE SER NUMÉRICO"
            enviaResposta(resposta, c)  
            conn.close()
            return            
        comando = "Select Codigo, Encerramento, Nome, Emissao from TDPFS Where Numero=%s"        
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        achou = False
        if row:
            chaveTdpf = row[0] #<-- chave do TDPF que será utilizado nas requisições aqui tratadas
            encerramento = row[1]
            emissao = row[3]
            nome = row[2]
            if nome==None or nome=="":
                nome = "NOME INDISPONÍVEL"
            nome = nome[:tamNome].ljust(tamNome)
            if encerramento!=None:
                msg = "TDPF encerrado"
                msg = msg.ljust(tamMsg)
                resposta = codigoStr+(("N"+msg+nome) if 2<=codigo<=5 else msg)
                enviaResposta(resposta, c) 
                conn.close()
                return               
        else: 
            msg = "TDPF NÃO foi localizado ou foi encerrado há muito tempo e retirado da base deste serviço"
            msg = msg.ljust(tamMsg)          
            resposta = codigoStr+(("I"+msg) if 2<=codigo<=5 else msg)
            enviaResposta(resposta, c) 
            conn.close()
            return              
        comando = "Select Alocacoes.Desalocacao from Alocacoes Where Alocacoes.Fiscal=%s and Alocacoes.TDPF=%s"
        cursor.execute(comando, (chaveFiscal, chaveTdpf))
        row = cursor.fetchone()    
        bSupervisor = False 
        if codigo in [4, 15]: #supervisor pode relacionar ciências ou atividades de TDPF
            bSupervisor, _ = verificaSupervisao(conn, cpf, tdpf)    
        achou = False
        if row and not bSupervisor:
            achou = True
            if row[0]!=None:
                msg = "CPF NÃO está mais alocado ao TDPF ou não é supervisor, em requisições em que isso seria relevante"
                msg = msg.ljust(tamMsg)                
                resposta = codigoStr+(("N"+msg+nome) if 2<=codigo<=5 else msg)
                enviaResposta(resposta, c)  
                conn.close()
                return                
        if not achou and not bSupervisor:
            msg = "CPF NÃO está alocado ao TDPF ou não é supervisor, em requisições em que isso seria relevante"
            msg = msg.ljust(tamMsg)            
            resposta = codigoStr+(("N"+msg+nome) if 2<=codigo<=5 else msg)     
            enviaResposta(resposta, c)   
            conn.close()
            return            
    
    comando = "Insert Into Log (IP, Requisicao, Mensagem, Data) Values (%s, %s, %s, %s)"
    try:
        #cursor.execute(comando, (c.getpeername()[0], codigo, msgRecebida[2:], datetime.now()))
        #conn.commit()
        pass #<--- IMPLEMENTAR O LOG (?)
    except:
        logging.info("Falhou o log - IP: "+c.getpeername()[0]+"; Msg: "+msgRecebida)
        conn.rollback()
    
    if codigo==2: #informa data de ciência relativa a TDPF 
        try:   #deve enviar imediatamente a descrição do documento que efetivou a ciência (sem criptografia)
            mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao
        except:
            c.close()
            logging.info("Erro de time out A - provavelmente cliente não respondeu no prazo. Abandonando operação.")
            conn.close()
            return         
        if len(msgRecebida)!=(39+tamChave): #inclui o tdpf e a data
            resposta = "99REQUISIÇÃO INVÁLIDA (4)"
            enviaResposta(resposta, c) 
            conn.close()
            return      
        
        data = msgRecebida[-10:] 
        if not isDate(data):
            msg = "Data de ciência inválida"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome 
            enviaResposta(resposta, c) 
            conn.close()
            return
        dataObj = datetime.strptime(data, '%d/%m/%Y')
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
        comando = "Select Data from Ciencias Where TDPF=%s and Data>=%s Order by Data DESC"
        cursor.execute(comando, (chaveTdpf, dataObj.date()))
        row = cursor.fetchone()
        if row:
            msg = "Data de ciência informada DEVE ser posterior à ultima informada para o TDPF ("+row[0].strftime('%d/%m/%Y')+")"
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return             
        requisicao = descriptografa(mensagemRec) 
        codReq = ""
        if len(requisicao)>=2:
            codReq = requisicao[:2]
        if len(requisicao)!=52 or codReq!="31":
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
        if len(documento)>50:
            msg = "Documento tem mais de 50 caracteres."
            msg = msg.ljust(tamMsg)             
            resposta = "02N"+msg+nome
            enviaResposta(resposta, c)  
            conn.close()
            return                                          
        tdpfMonitorado, monitoramentoAtivo, chave = tdpfMonitoradoCPF(conn, tdpf, cpf)
        try:
            comando = "Insert into Ciencias (TDPF, Data, Documento) Values (%s, %s, %s)"
            cursor.execute(comando, (chaveTdpf, dataObj.date(), documento))
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
        if rows==None:
            msg  = "Não há data de ciência informada para o TDPF" #Não havia data de ciência para o TDPF
            msg = msg.ljust(tamMsg)             
            resposta = "03N"+msg+nome
            enviaResposta(resposta, c)     
            conn.close()
            return            
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
        comando = "Select Data, Documento from Ciencias Where TDPF=%s Order by Data"
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
            documento = documento.ljust(50)
            datas = datas + row[0].strftime('%d/%m/%Y')+documento
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
            resposta = "99REQUISIÇÃO INVÁLIDA (6)"
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
        if rows==None:
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
        elif tam>=10:
            nn = str(tam)
        else:
            nn = "0"+str(tam)
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
            comando = "Select Data, Documento from Ciencias Where TDPF=%s order by Data DESC"
            cursor.execute(comando, (chaveTdpf,))
            cienciaReg = cursor.fetchone() #busca a data de ciência mais recente (DESC acima)
            documento = ""
            documento = documento.ljust(50)             
            if cienciaReg: 
                if cienciaReg[1]!=None:
                    documento = cienciaReg[1].ljust(50)               
                ciencia = cienciaReg[0] #obtem a data de ciência mais recente
                if ciencia!=None:
                    cienciaStr = ciencia.strftime('%d/%m/%Y')                    
                    registro = registro + tdpf + nome + emissao + vencimento + cienciaStr + documento   
                else:
                    registro = registro + tdpf + nome + emissao + vencimento + "00/00/0000" + documento            
            else:
                registro = registro + tdpf + nome + emissao + vencimento + "00/00/0000" + documento
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
                        requisicao = descriptografa(mensagemRec)
                        if requisicao!="0612345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (5)"
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

    if codigo==12: #Relação de TDPFs Alocados ao CPF e em andamento (não encerrados) - indica se está sendo monitorado e se é supervisor
        if len(msgRecebida)>(13+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (12)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        comando = """Select TDPFS.Numero, Alocacoes.Supervisor, TDPFS.Nome, TDPFS.Codigo from Alocacoes, TDPFS 
                   Where Alocacoes.Fiscal=%s and Alocacoes.Desalocacao Is Null and Alocacoes.TDPF=TDPFS.Codigo and 
                   TDPFS.Encerramento Is Null Order by TDPFS.Numero"""
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
        elif tam>=10:
            nn = str(tam)
        else:
            nn = "0"+str(tam)            
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
            nome = nome[:tamNome].ljust(tamNome)  
            registro = registro + tdpf+nome              
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
            registro = registro + supervisor    
            i+=1
            total+=1
            if i==5 or total==tam: #de cinco em cinco ou no último registro, enviamos
                enviaRespostaSemFechar(resposta+registro, c)
                resposta = "12"
                registro = ""
                i = 0
                if total==tam:
                    c.close()
                    break #percorreu os registros ou 99 deles, que é o limite
                if total<tam: #ainda não chegou ao final - aguardamos a requisição da continuação
                    try:
                        mensagemRec = c.recv(1024) #.decode('utf-8') #chegou a requisicao
                        requisicao = descriptografa(mensagemRec)
                        if requisicao!="1212345678909":
                            resposta = "99REQUISIÇÃO INVÁLIDA (5)"
                            enviaResposta(resposta, c) 
                            conn.close()
                            return
                    except:
                        c.close()
                        conn.close()
                        logging.info("Erro de time out 12 - provavelmente cliente não respondeu no prazo. Abandonando operação (2).")
                        return 

    if codigo==13: #mostra lista de tdpfs ativos e últimas ciências sob supervisão do CPF - semelhante ao código 6
        if len(msgRecebida)!=(16+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (13A)"
            enviaResposta(resposta, c) 
            conn.close()
            return         
        regInicial = msgRecebida[-3:]
        if not regInicial.isdigit():
            resposta = "99REQUISIÇÃO INVÁLIDA (13B)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        try:
            regInicial = int(regInicial)            
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA (13C)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if regInicial>0: #se foi informado o registro, devemos buscar a partir dele, limitado a dez
            offsetReg = "Limit 10 Offset "+str(regInicial-1)             
        else: #caso contrário, buscamos todos para informar a quantidade total que existe, mas só enviamos 10 (conforme if ao final do for abaixo)
             offsetReg = ""
        logging.info("Offset: "+offsetReg)      
        comando = """Select TDPFS.Numero, TDPFS.Nome, TDPFS.Vencimento, TDPFS.Emissao, TDPFS.Codigo 
                     from TDPFS, Supervisores 
                     Where Supervisores.Fiscal=%s and Supervisores.Equipe=TDPFS.Grupo and TDPFS.Encerramento Is Null Order by TDPFS.Numero """+offsetReg
        cursor.execute(comando, (chaveFiscal,))
        rows = cursor.fetchall()
        if rows:
            tam = len(rows)
        else:
            tam = 0
        if tam==0:
            resposta = "13000"
            enviaResposta(resposta, c) 
            conn.close()
            return             
        if tam>=1000: #limite de 999 tdpfs
            nnn = "999"
            tam = 999
        else:
            nnn= str(tam).rjust(3, "0")
        c.settimeout(10)
        registro = "" 
        if regInicial==0: 
            resposta = "13"+nnn #código da resposta e qtde de TDPFs que serão retornados
        else:
            resposta = "13"
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
            comando = "Select Data, Documento from Ciencias Where TDPF=%s order by Data DESC"
            cursor.execute(comando, (chaveTdpf,))
            cienciaReg = cursor.fetchone() #busca a data de ciência mais recente (DESC acima)
            if cienciaReg: 
                ciencia = dataTexto(cienciaReg[0]) #obtem a data de ciência mais recente
                documento = cienciaReg[1]
                if documento==None:
                    documento = ""
                documento = documento.ljust(50)                                   
                registro = registro + tdpf + nome + emissao + vencimento + ciencia + documento  
            else:
                registro = registro + tdpf + nome + emissao + vencimento + "00/00/0000"+" ".ljust(50) #provavelmente nenhum fiscal iniciou monitoramento
            #verifica se o TDPF está sendo monitorado    
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
            registro = registro+nomeFiscal
            #logging.info(registro)
            total+=1
            i+=1
            if i%10==0 or total==tam: #de cinco em cinco ou no último registro enviamos a mensagem
                enviaResposta(resposta+registro, c)
                return                             

    if codigo==14: #inclui atividade em um tdpf
        if len(msgRecebida)!=(112+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (14A)"
            enviaResposta(resposta, c) 
            conn.close()
            return          
        atividade = msgRecebida[-83:-33].strip()     
        if len(atividade)<4:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE - DESCRIÇÃO CURTA (14C)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        inicio = msgRecebida[-33:-23]
        try:
            inicio = datetime.strptime(inicio, "%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO INVÁLIDA (14D)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if inicio>datetime.now():  
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO FUTURA (14D1)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        if inicio.date()<emissao.date(): #ciência não pode ser inferior à data de emissão (obtida nas verificacões gerais)
            resposta = "14Data de início da atividade não pode ser inferior à de emissão do TDPF ("+emissao.strftime("%d/%m/%Y")+")"
            enviaResposta(resposta, c)  
            conn.close()
            return                                               
        vencimento = msgRecebida[-23:-13]
        try:
            vencimento = datetime.strptime(vencimento,"%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO INVÁLIDA (14E)"
            enviaResposta(resposta, c) 
            conn.close()
            return        
        if inicio>vencimento or vencimento.date()<datetime.now().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO ANTERIOR À DE INÍCIO OU PASSADA (14F)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        termino = msgRecebida[-13:-3]
        horas = msgRecebida[-3:]
        if termino!="00/00/0000":
            try:
                terminoAux = datetime.strptime(termino, "%d/%m/%Y")
            except:
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO INVÁLIDA (14G)"
                enviaResposta(resposta, c) 
                conn.close()
                return        
            if inicio>terminoAux or terminoAux>datetime.now():
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
            comando = "Insert Into Atividades (TDPF, Atividade, Inicio, Vencimento, Termino, Horas) Values (%s, %s, %s, %s, %s, %s)"           
            cursor.execute(comando, (chaveTdpf, atividade, inicio, vencimento, terminoAux, horas))  #chaveTdpf vem da rotina comum às requisições 2-5 e 14-17
            resposta = "14REGISTRO INCLUÍDO"            
            if tdpfMonitorado and monitoramentoAtivo==False: #monitoramento do tdpf estava desativado - ativa
                comando = "Update CadastroTDPFs Set Fim=Null Where Codigo=%s"
                cursor.execute(comando, (chave,))  
                resposta = resposta + " - MONITORAMENTO REATIVADO"                       
            elif not tdpfMonitorado: #tdpf não estava sendo monitorado - inclui ele
                comando = "Insert into CadastroTDPFs (Fiscal, TDPF, Inicio) Values (%s, %s, %s)"
                cursor.execute(comando, (chaveFiscal, chaveTdpf, datetime.today().date())) #chaveFiscal vem para todas as funções; chaveTdpf vem da rotina comum às requisições 2-5 e 14-17
                resposta = resposta + " - MONITORAMENTO INICIADO"                    
            conn.commit()
        except:
            conn.rollback()
            resposta = "14ERRO NA INCLUSÃO DO REGISTRO"
        enviaResposta(resposta, c)  
        conn.close()
        return             


    if codigo==15: #mostra lista de atividades de um tdpf - cpf deve estar alocado ou ser supervisor
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
        haTDPFS = False
        #verificamos se o fiscal está alocado
        haTDPFS, nome = verificaAlocacao(conn, cpf, tdpf)
        if not haTDPFS: #verificamos se é supervisor  
            haTDPFS, nome = verificaSupervisao(conn, cpf, tdpf)      
        if not haTDPFS:
            resposta = "15CPF NÃO ESTÁ ALOCADO OU NÃO É SUPERVISOR OU TDPF ENCERRADO/INEXISTENTE"
            enviaResposta(resposta, c) 
            conn.close()
            return
        #if nome==None:
        #    nome = ""   
        #nome = nome[:tamNome].ljust(tamNome)      
        #vencimento = dataTexto(row[2])             
        #emissao = dataTexto(row[3])  
        #consulta as atividades
        comando = "Select Codigo, Atividade, Vencimento, Inicio, Termino, Horas from Atividades Where TDPF=%s Order by Inicio "+offsetReg
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
            registro = registro + codigoAtiv + atividade + inicio + termino + vencimentoAtiv + horas
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
        if len(msgRecebida)!=(122+tamChave):
            resposta = "99REQUISIÇÃO INVÁLIDA (17A)"
            enviaResposta(resposta, c) 
            conn.close()
            return   
        codAtividade = msgRecebida[-93:-83].strip()     
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
        atividade = msgRecebida[-83:-33].strip()     
        if len(atividade)<4:
            resposta = "99REQUISIÇÃO INVÁLIDA - ATIVIDADE - DESCRIÇÃO CURTA (17E)"
            enviaResposta(resposta, c) 
            conn.close()
            return                  
        inicio = msgRecebida[-33:-23]
        try:
            inicio = datetime.strptime(inicio, "%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO INVÁLIDA (17F)"
            enviaResposta(resposta, c) 
            conn.close()
            return
        if inicio>datetime.now():  
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE INÍCIO FUTURA (17G)"
            enviaResposta(resposta, c) 
            conn.close()
            return  
        if inicio.date()<emissao.date(): #ciência não pode ser inferior à data de emissão (obtida nas verificacões gerais)
            resposta = "17Data de início da atividade não pode ser inferior à de emissão do TDPF ("+emissao.strftime("%d/%m/%Y")+")"
            enviaResposta(resposta, c)  
            conn.close()
            return                                            
        vencimento = msgRecebida[-23:-13]
        try:
            vencimento = datetime.strptime(vencimento,"%d/%m/%Y")
        except:
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO INVÁLIDA (17H)"
            enviaResposta(resposta, c) 
            conn.close()
            return        
        if inicio>vencimento or vencimento.date()<datetime.now().date():
            resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE VENCIMENTO ANTERIOR À DE INÍCIO OU PASSADA (17I)"
            enviaResposta(resposta, c) 
            conn.close()
            return 
        termino = msgRecebida[-13:-3]
        if termino!="00/00/0000":
            try:
                terminoAux = datetime.strptime(termino, "%d/%m/%Y")
            except:
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO INVÁLIDA (17J)"
                enviaResposta(resposta, c) 
                conn.close()
                return        
            if inicio>terminoAux or terminoAux>datetime.now():
                resposta = "99REQUISIÇÃO INVÁLIDA - DATA DE TÉRMINO ANTERIOR À DE INÍCIO OU FUTURA (17K)"
                enviaResposta(resposta, c) 
                conn.close()
                return
        else:
            terminoAux = None
        horas = msgRecebida[-3:]
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
            comando = "Update Atividades Set TDPF=%s, Atividade=%s, Inicio=%s, Vencimento=%s, Termino=%s, Horas=%s Where Codigo=%s"           
            cursor.execute(comando, (chaveTdpf, atividade, inicio, vencimento, terminoAux, horas, codAtividade))                
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
        for parte in range(1, numPartes+1):    
            enviaRespostaSemFechar(resposta, c)   
            logging.info("Enviou")              
            try:    
                c.settimeout(10)            
                mensagemRec = c.recv(4096) #chegou a requisicao sem criptografia
            except:
                c.close()
                conn.close()
                logging.info("Erro de time out - provavelmente cliente não respondeu no prazo. Abandonando operação (Req 18).")
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
        for entrada in entradas:
            mensagemRec = c.recv(1024).decode('utf-8') #chegou a requisicao sem criptografia            
            if mensagemRec[:2]!="19" or mensagemRec[2:4]!="11":
                enviaResposta("99REQUISIÇÃO INVÁLIDA - AGUARDANDO PEDIDOS DE CONTINUAÇÃO (19C)", c) 
                conn.close()
                return             
            codReg = entrada[0]   
            data = entrada[1].strftime("%d/%m/%Y") 
            texto = entradasRaw[j][0]   
            j+=1              
            totalPartes = len(texto) // 500 #500 caracteres do texto são enviados de cada vez
            if (len(texto) % 500) > 0:
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
                    logging.info("Erro de time out - provavelmente cliente não respondeu no prazo. Abandonando operação (Req 19).")
                    return                     
                if mensagemRec[:2]!="19" or mensagemRec[2:4]!="11":
                    enviaResposta("99REQUISIÇÃO INVÁLIDA - AGUARDANDO PEDIDOS DE CONTINUAÇÃO (19D)", c) 
                    conn.close()
                    connRaw.close()
                    return 
                resposta = texto[i*500:(i*500+500)]
                c.sendall(resposta) 
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
        print(msgRecebida)      
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


    return #não chega aqui, mas ...
      
                             
def servidor(): 
    global  threads, s, pubKey

    s = socket.socket()          
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
    s.listen(5)      
    logging.info("socket is listening")
      
    #só fica escutando a rede, pega a mensagem e encaminha para tratamento 
    while True:  
        try:
            c, addr = s.accept()      
            logging.info('Got connection from ' + str(addr))    
            c.settimeout(10)
            try:
                if not estaoChavesValidas():
                    pubKey = geraChaves()
                msgRecebida = c.recv(2048)#.decode('utf-8') #recebe a mensagem  (era 1024)
                #logging.info(binascii.hexlify(msgRecebida))
                #logging.info(len(msgRecebida))
    
                threadTrata = threading.Thread(target=trataMsgRecebida, args=(msgRecebida,c))
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


chavesCripto = [] #contém a função de descriptografia [0] e a data/hora de vencimento [1] (2 h a partir da geração)
pubKey = geraChaves()
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
token = os.getenv("TOKEN", "ERRO")
ambiente = os.getenv("AMBIENTE", "TESTE")
conn = conecta() #testa a conexão com o BD
if conn!=None:
    conn.close()
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
    print("Corrija o problema do Banco de Dados e reinicie este serviço.")
