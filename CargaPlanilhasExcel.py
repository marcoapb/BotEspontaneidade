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
import logging  
import mysql.connector
from mysql.connector import errorcode
import schedule
import threading
import time
from datetime import datetime, timedelta

#bibliotecas necessárias para envio de e-mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import formatdate
from email import encoders
import smtplib


#rotina para envio de e-mail - retirado de Bot_Telegram.py
def enviaEmail(email, texto, assunto, arquivo=None): #envia email, conforme parâmetros - se passar o arquivo (caminho e nome), ele vai como anexo 
    try:
        #server = smtplib.SMTP('INETRFOC.RFOC.SRF: 25') #servidor de email Notes
        server = smtplib.SMTP('exchangerfoc.rfoc.srf: 25')
        #pass
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

def acrescentaZero(numero, n): 
    if numero==None:
        return  ""
    numero = str(numero)    
    if len(numero)<n:
        for i in range(len(numero), n):
            numero = "0"+numero  
    return numero

def acrescentaZeroCPF(cpf):
    return acrescentaZero(cpf, 11)  
      
def montaGrupoFiscal(linha):
    return acrescentaZero(linha['R028_GRSE_SUA_UA_CD'], 7) + acrescentaZero(linha['R028_GRSE_SUA_CD'], 4) + acrescentaZero(linha['R028_GRSE_CD'], 3)

#calcula DV de um CPF - retorna o CPF completo
def calculaDVCPF(cpfPar):
    cpf = getAlgarismos(cpfPar)
    if cpf==None:
        return None
    if len(cpf)>9:
        return None    
    if len(cpf)<9:
        cpf = acrescentaZero(cpf, 9)    
    # Calculado o primeiro DV
    calc = lambda i: int(cpf[i]) * (10-i)
    somaJ = sum(map(calc, range(9)))
    restoJ = somaJ % 11
    if (restoJ == 0 or restoJ == 1):
       j = 0
    else:
       j = 11 - restoJ   
    cpf=cpf+str(j)
    # Calculado o segundo DV
    calc2 = lambda i: int(cpf[i]) * (11-i)
    somaK = sum(map(calc2, range(9))) + j*2
    restoK = somaK % 11
    if (restoK == 0 or restoK == 1):
       k = 0
    else:
       k = 11 - restoK      
    cpf = cpf + str(k)
    return cpf

def realizaCargaDados():
    global dirExcel, termina, hostSrv, hora1
    try:
        dfTdpf = pd.read_excel(dirExcel+"TDPFS.xlsx", dtype={'Porte':object, 'Acompanhamento':object, 'Receita Programada(Tributo) Código': int})
        dfAloc = pd.read_excel(dirExcel+"ALOCACOES.xlsx")
        dfFiscais = pd.read_excel(dirExcel+"Fiscais.xlsx")
        dfSupervisores = pd.read_csv(dirExcel+"Supervisores.CSV", sep=";", encoding = "ISO-8859-1")
        dfOperacoes = pd.read_excel(dirExcel+"OPERACOES.xlsx")
    except:
        print("Erro no acesso aos arquivos xlsx e/ou csv ou só há arquivos já processados; outra tentativa será feita às "+hora1)
        logging.info("Arquivos Excel não foram encontrados (um ou mais) - TDPFs.xlsx, ALOCACOES.xlsx, Fiscais.xlsx, OPERACOES.xlsx ou Supervisores.CSV; outra tentativa será feita às "+hora1) 
        return
    dfFiscais['CPF']=dfFiscais['CPF'].astype(str).map(acrescentaZeroCPF) 
    dfAloc['Ind. RH Superv. Gr. Fiscal RPF'] = dfAloc['Ind. RH Superv. Gr. Fiscal RPF'].astype(str)
    dfSupervisores['CPF']=dfSupervisores['R028_RH_PF_NR'].astype(str).map(calculaDVCPF)
    dfSupervisores['Grupo Fiscal']=dfSupervisores.apply(montaGrupoFiscal, axis=1)
    #dfTdpf['Porte']=dfTdpf['Porte'].astype(str)
    #dfTdpf['Acompanhamento']=dfTdpf['Acompanhamento'].astype(str)
    #MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234") 
    #print(dfTdpf.head())
    #print(dfTdpf.dtypes)
    #print(dfAloc.head())
    #print(dfAloc.dtypes)
    #return
    try:
        logging.info("Conectando ao servidor de banco de dados ...")
        logging.info(MYSQL_DATABASE)
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
    print("Realizando carga dos dados em "+datetime.now().strftime("%d/%m/%Y %H:%M"))
    cursor = conn.cursor(buffered=True)
    #if hostSrv == 'localhost': #no ambiente local de testes (SO Windows), apagamos a base - comentar estas linhas, se necessário
    #    cursor.execute("Delete from TDPFS")
    #    cursor.execute("Delete from Alocacoes")
    #    cursor.execute("Delete from Supervisores")
    #    cursor.execute("Delete from Atividades")
    #    cursor.execute("Delete from Ciencias")
    #    cursor.execute("Delete from AvisosVencimento")
    #    cursor.execute("Delete from CadastroTDPFs")
    #    conn.commit()
    logging.info(dfTdpf.head())
    logging.info(dfAloc.head())
    logging.info(dfFiscais.head())
    logging.info(dfSupervisores.head())
    logging.info(dfOperacoes.head())

    logging.info(dfTdpf.dtypes)
    logging.info(dfAloc.dtypes)
    logging.info(dfFiscais.dtypes)
    logging.info(dfSupervisores.dtypes)
    logging.info(dfOperacoes.dtypes)

    selectFisc = "Select Codigo, CPF, Nome from Fiscais Where CPF=%s"
    insereFisc = "Insert Into Fiscais (CPF, Nome) Values (%s, %s)"

    selectTDPF = "Select Codigo, Grupo, Encerramento, Vencimento from TDPFS Where Numero=%s"
    insereTDPF = "Insert Into TDPFS (Numero, Grupo, Emissao, Nome, NI, Vencimento, Porte, Acompanhamento, Encerramento) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    atualizaTDPFEnc = "Update TDPFS Set Encerramento=%s Where Codigo=%s"
    atualizaTDPFGrupoVencto = "Update TDPFS Set Grupo=%s, Vencimento=%s Where Codigo=%s"

    selectAloc = "Select Codigo, Desalocacao, Supervisor from Alocacoes Where TDPF=%s and Fiscal=%s"
    insereAloc = "Insert Into Alocacoes (TDPF, Fiscal, Alocacao, Supervisor, Horas) Values (%s, %s, %s, %s, %s)"
    insereAlocDesaloc = "Insert Into Alocacoes (TDPF, Fiscal, Alocacao, Desalocacao, Supervisor, Horas) Values (%s, %s, %s, %s, %s, %s)"
    atualizaAloc = "Update Alocacoes Set Desalocacao=%s, Supervisor=%s, Horas=%s Where Codigo=%s"
    atualizaAlocHoras = "Update Alocacoes Set Horas=%s Where Codigo=%s"

    selectCiencias = "Select * from Ciencias Where TDPF=%s" 
    insereCiencia = "Insert Into Ciencias (TDPF, Data, Documento) Values (%s, %s, %s)"

    selectOperacoes = "Select Operacoes.Codigo, OperacoesFiscais.Operacao, PeriodoInicial, PeriodoFinal, Tributos.Tributo from Operacoes, OperacoesFiscais, Tributos Where Operacoes.TDPF=%s and Operacoes.Operacao=OperacoesFiscais.Codigo and Operacoes.Tributo=Tributos.Codigo"
    insereOperacao = "Insert Into Operacoes (TDPF, Operacao, PeriodoInicial, PeriodoFinal, Tributo) Values (%s, %s, %s, %s, %s)"
    apagaOperacao = "Delete from Operacoes Where Codigo=%s"

    selectOpFiscal = "Select Codigo from OperacoesFiscais Where Operacao=%s"
    insertOpFiscal = "Insert Into OperacoesFiscais (Operacao, Descricao, Valor) Values (%s, %s, %s)"

    selectTributo = "Select Codigo from Tributos Where Tributo=%s"
    insertTributo = "Insert Into Tributos (Tributo, Descricao) Values (%s, %s)"

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
    atualizou = False    
    for linha in range(dfTdpf.shape[0]): #percorre os TDPFs das planilhas Excel
        if (linha+1)%500==0:
            print("Processando TDPF nº "+str(linha+1)+" de "+str(dfTdpf.shape[0]))
        tdpfAux = dfTdpf.iat[linha,0]
        tdpf = getAlgarismos(tdpfAux)
        distribuicao = dfTdpf.iat[linha, 9] #na verdade, aqui é a data de assinatura/emissão do TDPF (antes tinha apenas a distribuição) 
                                            #<- a assinatura revelou-se pior que a distribuição - voltei a usar esta
        inicio = dfTdpf.iat[linha, 11]
        encerramento = dfTdpf.iat[linha, 12]
        #situacao = dfTdpf.iloc[linha, 13]
        ni = dfTdpf.iat[linha, 17]
        nome = dfTdpf.iat[linha, 18]
        porte = dfTdpf.iat[linha, 27]
        acompanhamento = dfTdpf.iat[linha, 28]
        if porte==np.nan or pd.isna(porte) or porte=="":
            porte = None
        if acompanhamento==np.nan or pd.isna(acompanhamento) or acompanhamento=="":
            acompanhamento = None      
        #comentei o trecho abaixo pq já vem certinho na planilha do Excel
        #tipo = str(type(porte)).upper()
        #if "STR" in tipo or "UNICODE" in tipo: 
        #    porte = porte[:3]    
        #tipo = str(type(acompanhamento)).upper()
        #if "STR" in tipo or "UNICODE" in tipo: 
        #    acompanhamento = acompanhamento[:1]                       
        cursor.execute(selectTDPF, (tdpf,))
        regTdpf = cursor.fetchone()  
        #precisamos incluir os TDPFs encerrados a partir do ano de entrada em produção (2021)
        if not regTdpf and encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None:    
            if paraData(encerramento).year<2021: #se foi encerrado antes de 2021 (entrada em produção), desprezamos
                continue
        if regTdpf: #TDPF consta da base
            chaveTdpf = regTdpf[0]  #chave do registro do TDPF  - para poder atualizar o registro, se for necessário          
            if regTdpf[2]!=None:
                continue #TDPF já encerrado na base - não há interesse em atualizar
        else: #TDPF não existe na base - pedi para ajustar o relatório do gerencial para incluir os encerrados a partir de 2021 (o problema abaixo não deve ocorrer)
            if porte==None or acompanhamento==None: #porte ou acompanhamento especial não foram obtidos do gerencial Ação Fiscal no DW - significa que não há necessidade de 
                                                    #incluir o TDPF na base pois já está encerrado (por isso não consta do gerencial)
                logging.info(f"TDPFs: {tdpfAux} não tem monitoramento e/ou porte sem constar na base - TDPF foi desprezado.")
                continue            
        df = dfAloc.loc[dfAloc['Número do RPF Expresso']==tdpfAux] #selecionamos as alocações do TDPF
        if df.shape[0]==0:
            logging.info(f"TDPFs: {tdpfAux} não tem fiscal alocado - TDPF foi desprezado.")
            continue
        if distribuicao: #calculamos a data de vencimento a partir da data de distribuição - o ideal seria calcular a partir da data de emissão, mas o DW não tem a informação
            distData = paraData(distribuicao) 
            vencimento = distData + timedelta(days=(120-1)) #funciona assim no Ação Fiscal - o primeiro vencimento, ocorre 119 dias (conta o dia da emissão); os subsequentes, 120 dias
            while vencimento.date()<datetime.now().date():
                vencimento = vencimento + timedelta(days=120)
                if encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None:
                    if vencimento.date()>paraData(encerramento).date(): #assim que passou do encerramento, paramos de acrescentar 120 dias ao vencimento
                        break
        else: #distribuição nulo? Não deve acontecer, mas ...
            vencimento = None   
            distData = None   
        #precisamos percorrer as alocações para descobrir o grupo atual
        grupoAtu = None 
        for linha2 in range(df.shape[0]):
            grupo = df.iat[linha2, 4]
            tipoGrupo = str(type(grupo)).upper()
            if "STR" in tipoGrupo or "UNICODE" in tipoGrupo:
                grupo = getAlgarismos(grupo)
            else:
                grupo = None
            desalocacao = df.iat[linha2, 10]            
            if (desalocacao=="SD" or desalocacao=="" or paraData(desalocacao)==None) and grupo!="" and grupo!=None:
                grupoAtu = grupo 
                break 
        bInseriuCiencia = False                
        if not regTdpf: #TDPF não consta da base
            tabTdpfs+=1
            atualizou = True
            cursor.execute(insereTDPF, (tdpf, grupoAtu, distData, nome, ni, vencimento, porte, acompanhamento, paraData(encerramento)))
            cursor.execute(selectTDPF, (tdpf,))
            regTdpf = cursor.fetchone()   
            chaveTdpf = regTdpf[0]  #chave do registro do TDPF                      
            if inicio!="SD" and inicio!="" and paraData(inicio)!=None:
                tabCiencias+=1
                cursor.execute(insereCiencia, (chaveTdpf, paraData(inicio), "ACÃO FISCAL"))
                bInseriuCiencia = True
        elif regTdpf[2]==None and encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None: #TDPF existia na base em andamento, agora é encerrado
            tabTdpfsAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFEnc, (paraData(encerramento), chaveTdpf))
        elif regTdpf[1]!=grupoAtu or regTdpf[3].date()<datetime.now().date(): #mudou o grupo e/ou TDPF está vencido, mas, em ambos os casos, NÃO encerrado - atualiza grupo e vencimento
            gruposAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFGrupoVencto, (grupoAtu, vencimento, chaveTdpf))
        if regTdpf and inicio!="SD" and inicio!="" and paraData(inicio)!=None and not bInseriuCiencia:
            cursor.execute(selectCiencias, (chaveTdpf,))
            regCiencia = cursor.fetchone()
            if not regCiencia:
                tabCiencias+=1
                atualizou = True
                cursor.execute(insereCiencia, (chaveTdpf, paraData(inicio), "ACÃO FISCAL"))                
        for linha2 in range(df.shape[0]): #percorre as alocações na planilhas Excel relativas ao TDPF do loop externo
            cpf = getAlgarismos(df.iat[linha2, 6])
            fiscal = df.iat[linha2, 7] #nome do fiscal
            alocacao = df.iat[linha2, 9]
            desalocacao = df.iat[linha2, 10]  
            supervisor = df.iat[linha2, 12]
            if supervisor==np.nan or pd.isna(supervisor) or supervisor=="" or supervisor==None:
                supervisor = "N"
            else:
                supervisor = supervisor[:1]
            horas = df.iat[linha2, 16]
            try:    
                horas = int(horas)    
            except:
                horas = 0    
            dfFiscal = dfFiscais.loc[dfFiscais['CPF']==cpf]
            email = None
            if dfFiscal.shape[0]>0:
                if dfFiscal.iat[0, 4]!=np.nan and not pd.isna(dfFiscal.iat[0, 4]) and dfFiscal.iat[0, 4]!="": #email está na coluna 4 (coluna 'E' do Excel)
                    email = dfFiscal.iat[0, 4]
            #print(email)        
            cursor.execute(selectUsuario, (cpf,))
            regUser = cursor.fetchone()
            if regUser!=None: #achou o usuário - vemos se tem e-mail cadastrado
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
                cursor.execute(selectFisc, (cpf,))
                regFisc = cursor.fetchone()
            chaveFiscal = regFisc[0] #chave do registro do Fiscal (inserido ou consultado anteriormente)
            cursor.execute(selectAloc, (chaveTdpf, chaveFiscal))
            regAloc = cursor.fetchone()
            if not regAloc:
                tabAloc+=1
                if desalocacao=="SD" or desalocacao=="" or paraData(desalocacao)==None:
                    atualizou = True
                    cursor.execute(insereAloc, (chaveTdpf, chaveFiscal, paraData(alocacao), supervisor, horas))
                else:
                    atualizou = True
                    cursor.execute(insereAlocDesaloc, (chaveTdpf, chaveFiscal, paraData(alocacao), paraData(desalocacao), supervisor, horas))
            elif regAloc[1]==None and desalocacao!="SD" and desalocacao!="" and paraData(desalocacao)!=None:
                tabAlocAtu+=1
                atualizou = True
                cursor.execute(atualizaAloc, (paraData(desalocacao), supervisor, horas, regAloc[0]))
            elif regAloc[1]!=None and (desalocacao=="SD" or desalocacao=="" or paraData(desalocacao)==None):
                tabAlocAtu+=1
                atualizou = True
                cursor.execute(atualizaAloc, (None, supervisor, horas, regAloc[0]))                
            elif regAloc[2]!=supervisor:
                tabAlocAtu+=1
                atualizou = True
                cursor.execute(atualizaAloc, (regAloc[1], supervisor, horas, regAloc[0]))  
            else:
                cursor.execute(atualizaAlocHoras, (horas, regAloc[0]))    
                tabAlocAtu+=1
                atualizou = True 
        #percorremos as operações do TDPF - excluímos as que não mais existirem e incluímos as que não existirem
        dfOp = dfOperacoes.loc[dfOperacoes['Número do RPF Expresso']==tdpfAux] #selecionamos as operações do TDPF
        cursor.execute(selectOperacoes, (chaveTdpf,))
        regOperacoes = cursor.fetchall()
        opExistentes = []
        for regOperacao in regOperacoes: #atualizamos as operações que mudaram algo no período ou excluímos aquelas que não existem mais
            operacao = regOperacao[1]
            codigoOperacao = regOperacao[0]
            perInicial = regOperacao[2]
            perFinal = regOperacao[3]
            tributo = regOperacao[4]
            operTrib = str(operacao).rjust(6, "0")+str(tributo).rjust(5, "0")
            dfOpAux = dfOp.loc[dfOp['Operação Fiscal Atual Código']==operacao]
            if dfOpAux.shape[0]>0: #operação existe no TDPF - temos que ver se há alguma divergência no período (aumentou ou diminuiu)
                opExistentes.append(operTrib)
                menorMes = paraData(dfOpAux.loc[dfOpAux['Mês Início'].idxmin()]["Mês Início"])
                maiorMes = paraData(dfOpAux.loc[dfOpAux['Mês Fim'].idxmax()]["Mês Fim"])
                if maiorMes!=perFinal or menorMes!=perInicial:
                    comando = "Update Operacoes Set PeriodoInicial=%s, PeriodoFinal=%s Where Codigo=%s"
                    cursor.execute(comando, (menorMes, maiorMes, codigoOperacao))
            else:
                cursor.execute(apagaOperacao, (codigoOperacao,)) #operação foi removida do TDPF - removemos ela da base
        #incluímos as operações do TDPF (as que não tiverem sido cadastradas)
        for linha2 in range(dfOp.shape[0]):
            operacao = int(dfOp.iat[linha2, 8])
            valor = dfOp.iat[linha2, 11] #peso/valor da operação
            tributo = int(dfOp.iat[linha2, 1])
            operTrib = str(operacao).rjust(6, "0")+str(tributo).rjust(5, "0")
            if operTrib in opExistentes: #operação/tributo já está na base
                continue
            opExistentes.append(operTrib)
            #não está na base - temos que incluí-la
            #consultamos o tributo e o incluímos, se não existir           
            cursor.execute(selectTributo, (tributo,))
            rowTributo = cursor.fetchone()
            if not rowTributo:
                cursor.execute(insertTributo, (tributo, dfOp.iat[linha2, 2].upper()))
                cursor.execute(selectTributo, (tributo,))
                rowTributo = cursor.fetchone()
            codTributo = rowTributo[0]
            #consultamos a operação fiscal e a incluímos, se não existir
            cursor.execute(selectOpFiscal, (operacao,))
            rowOperacao = cursor.fetchone()
            if not rowOperacao:
                cursor.execute(insertOpFiscal, (operacao, dfOp.iat[linha2, 9].upper(), float(valor))) #tirei o tributo
                cursor.execute(selectOpFiscal, (operacao,))
                rowOperacao = cursor.fetchone()
            codOperacao = rowOperacao[0]
            #inserimos a operação vinculada ao TDPF
            dfOpAux = dfOp.loc[dfOp['Operação Fiscal Atual Código']==operacao]
            #selecionamos o menor e o maior mês do período da operação deste TDPF            
            if dfOpAux.shape[0]>0:
                perInicial = paraData(dfOpAux.loc[dfOpAux['Mês Início'].idxmin()]["Mês Início"])
                perFinal = paraData(dfOpAux.loc[dfOpAux['Mês Fim'].idxmax()]["Mês Fim"])            
            cursor.execute(insereOperacao, (chaveTdpf, codOperacao, perInicial, perFinal, codTributo)) #incluí o tributo
    if termina:
        return
    #atualizamos a tabela de supervisões de grupos/equipes fiscais (Supervisores)
    comando = "Select Distinctrow Grupo from TDPFS"
    cursor.execute(comando)
    gruposRows = cursor.fetchall()
    tabGrupos = 0
    tabGruposAtu = 0
    atualizouSuperv = False
    print("Processados "+str(dfTdpf.shape[0])+" TDPFS.")
    print("Atualizando supervisores ...")
    superv = 0 #número de supervisores que não fazem parte de nenhum grupo - são incluídos na tabela de usuários pq supervisionam ativamente alguma equipe
    for grupoRow in gruposRows:
        df = dfSupervisores.loc[dfSupervisores['Grupo Fiscal']==grupoRow[0]].sort_values(by=['R028_DT_INI_VINCULO'], ascending=False)
        if df.shape[0]>0: #pega só o último registro da supervisão da equipe (mais recente início na supervisão)
            cpf = df.iat[0, 12] 
            nomeSuperv = df.iat[0, 8]
            dataIni = df.iat[0, 9]
            dataFim = df.iat[0, 10]
            #selectFisc = "Select Codigo, CPF, Nome from Fiscais Where CPF=%s"
            #insereFisc = "Insert Into Fiscais (CPF, Nome) Values (%s, %s)"
            cursor.execute(selectFisc, (cpf,))     
            fiscalRow = cursor.fetchone()
            if not fiscalRow:
                cursor.execute(insereFisc, (cpf, nomeSuperv))
                cursor.execute(selectFisc, (cpf,))     
                fiscalRow = cursor.fetchone()
            chaveFiscal = fiscalRow[0]               
            if dataFim==None or dataFim=="" or np.isnan(dataFim) or dataFim==float(0): #é supervisor da equipe
                #print(grupoRow[0]+" - "+cpf)                     
                comando = "Select Codigo, Fiscal, Inicio, Fim from Supervisores Where Equipe=%s and Fiscal=%s and Fim Is Null"
                cursor.execute(comando, (grupoRow[0], chaveFiscal))
                supervisoresRows = cursor.fetchall()
                bAchou = True
                if supervisoresRows==None:
                    bAchou = False
                elif len(supervisoresRows)==0:
                    bAchou = False
                #print(bAchou)    
                if not bAchou: #ainda não consta da tabela de Supervisores
                    comando = "Insert Into Supervisores (Equipe, Fiscal, Inicio) Values (%s, %s, %s)"
                    tabGrupos+=1
                    cursor.execute(comando, (grupoRow[0], chaveFiscal, paraData(dataIni)))
                    #verificamos se este grupo não tem outro supervisor ativo - se tiver, colocamos a data final - fazemos isso para garantir caso haja uma descontinuidade
                    #não obtida pelo else abaixo
                    comando = "Select Codigo from Supervisores Where Equipe=%s and Fiscal<>%s and Fim Is Null"
                    cursor.execute(comando, (grupoRow[0], chaveFiscal))                    
                    supervisoresRows = cursor.fetchall()
                    if supervisoresRows!=None:
                        if len(supervisoresRows)>0:
                            comando = "Update Supervisores Set Fim=%s Where Equipe=%s and Fiscal<>%s and Fim Is Null" #"matamos" os antigos supervisores
                            cursor.execute(comando, (datetime.now().date(), grupoRow[0], chaveFiscal))
                #verificamos se este supervisor consta da tabela de usuários
                dfFiscal = dfFiscais.loc[dfFiscais['CPF']==cpf]
                email = None
                if dfFiscal.shape[0]>0: 
                    if dfFiscal.iat[0, 4]!=np.nan and not pd.isna(dfFiscal.iat[0,4]) and dfFiscal.iat[0, 4]!="": #email está na coluna 4 (coluna 'E' do Excel)
                        email = dfFiscal.iat[0, 4]                                   
                    cursor.execute(selectUsuario, (cpf,))
                    rows = cursor.fetchall()
                    bAchou = True
                    if rows==None:
                        bAchou = False
                    elif len(rows)==0:
                        bAchou = False
                    if not bAchou:  #não existe o supervisor na tabela de usuários             
                        cursor.execute(insereUsuario, (cpf, email)) 
                        atualizouSuperv = True                               
            else: #não é mais supervisor da equipe
                comando = "Select Codigo, Fiscal, Inicio, Fim from Supervisores Where Equipe=%s and Fiscal=%s and Inicio=%s and Fim Is Null"
                cursor.execute(comando, (grupoRow[0], chaveFiscal, paraData(dataIni)))
                supervisoresRows = cursor.fetchall()
                bAchou = True
                if supervisoresRows==None:
                    bAchou = False
                elif len(supervisoresRows)==0:
                    bAchou = False
                if bAchou:
                    comando = "Update Supervisores Set Fim=%s Where Codigo=%s"
                    cursor.execute(comando,(paraData(dataFim), supervisoresRows[0][0]))
                    tabGruposAtu+=1
        else:
            logging.info("Grupo não encontrado: "+grupoRow[0])

    if tabGrupos>0 or tabGruposAtu>0 or atualizouSuperv or atualizou:
        try:
            conn.commit()  
            logging.info("Registros Incluídos:")  
            logging.info(f"TDPFs: {tabTdpfs}")
            logging.info(f"Fiscais: {tabFiscais}")
            logging.info(f"Ciencias: {tabCiencias}")
            logging.info(f"Alocacoes: {tabAloc}")
            logging.info(f"Usuarios: {tabUsuarios}")  
            logging.info(f"Equipes: {tabGrupos}")   

            logging.info("Registros Atualizados:")
            logging.info(f"TDPFs: {tabTdpfsAtu}")
            logging.info(f"Grupos(TDPFs)/Vencimentos: {gruposAtu}")
            logging.info(f"Alocacoes: {tabAlocAtu}")
            logging.info(f"Usuarios: {tabUsuariosAtu}")
            logging.info(f"Equipes: {tabGruposAtu}")   
            textoUNacionais = "Carga de dados (TDPFs, Alocações e Operações) efetuada na base de dados - "+datetime.now().strftime("%d/%m/%Y %H:%M") +"\nRegistros Incluídos:\n" 
            textoUNacionais += f"TDPFs: {tabTdpfs}\n" + f"Fiscais: {tabFiscais}\n" + f"Ciencias: {tabCiencias}\n" + f"Alocacoes: {tabAloc}\n"
            textoUNacionais += f"Usuarios: {tabUsuarios}\n"
            textoUNacionais += f"Equipes: {tabGrupos}\nRegistros Atualizados:\n"
            textoUNacionais += f"TDPFs: {tabTdpfsAtu}\n" + f"Grupos(TDPFs)/Vencimentos: {gruposAtu}\n" + f"Usuarios: {tabUsuariosAtu}\n" + f"Equipes: {tabGruposAtu}\n"
            print("TDPFs, Alocacoes, Ciências e Supervisores/Equipes foram atualizados")          
        except:
            textoUNacionais = "Sr. Usuário,\n\nA tentativa de realizar a carga de dados (TDPFs, Alocações e Operações) FALHOU - "+datetime.now().strftime("%d/%m/%Y %H:%M") + "\n\nAtenciosamente,\n\nCofis/Disav"
            print("Erro ao tentar efetivar as atualizações no Banco de Dados - É necessário verificar o erro e tentar fazer novamente a carga.")
            logging.info("Erro ao tentar efetivar as atualizações no Banco de Dados - Nenhum dado foi atualizado")
            avisaUsuariosNacionais(textoUNacionais, cursor)
            conn.rollback()
            conn.close()
            return 
    print("Iniciando atualização do indicador de supervisor nos TDPFs")
    select = """Select Alocacoes.Codigo From Alocacoes, TDPFS, Supervisores 
                Where Alocacoes.TDPF=TDPFS.Codigo and TDPFS.Encerramento Is Null and 
                Supervisores.Equipe=TDPFS.Grupo and Supervisores.Fim Is Null and Supervisores.Fiscal=Alocacoes.Fiscal and Alocacoes.Desalocacao Is Null"""
    cursor.execute(select)
    rows = cursor.fetchall()
    lista = ""
    for row in rows:
        if lista!="":
            lista = lista +", "
        lista = lista + str(row[0])
    if lista!="":
        lista = "(" + lista +")"
        comando = "Update Alocacoes Set Supervisor='N' Where Alocacoes.Supervisor='S' and Alocacoes.Codigo Not In " + lista
        cursor.execute(comando) #indicador de supervisor 'N' nos TDPFS
        comando = "Update Alocacoes Set Supervisor='S' Where Alocacoes.Supervisor='N' and Alocacoes.Codigo In " + lista
        cursor.execute(comando) #indicador de supervisor 'S' nos TDPFS
        #comando = "Update Alocacoes Set Supervisor='N' Where Alocacoes.Desalocacao Is Not Null and Alocacoes.Supervisor='S'" 
        #fiscal desalocado não é supervisor
        #cursor.execute(comando)
        try:
            conn.commit()           
            print("Supervisores não alocados a TDPFs e incluídos: "+str(superv))                      
        except:
            print("Erro ao tentar efetivar as atualização do indicador de supervisor dos TDPFs - É necessário verificar o erro e tentar fazer novamente a carga.")
            logging.info("Erro ao tentar efetivar as atualização do indicador de supervisor dos TDPFs")
            conn.rollback()        
            logging.error(mysql.connector.Error)
            print(mysql.connector.Error)      
    try:
        os.rename(dirExcel+"TDPFS.xlsx", dirExcel+"TDPFS_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        os.rename(dirExcel+"ALOCACOES.xlsx", dirExcel+"ALOCACOES_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        logging.info("Arquivos renomeados")
        textoUNacionais += "Arquivos foram renomeados."
    except:
        print("Erro ao tentar renomear os arquivos")            
        logging.error("Erro ao tentar renomear os arquivos")   
        textoUNacionais += "Houve um erro ao tentar renomear os arquivos."                       
    print("Carga finalizada")
    textoUNacionais = "Sr. Usuário,\n\n" + textoUNacionais + "\n\nAtenciosamente,\n\nCofis/Disav"
    avisaUsuariosNacionais(textoUNacionais, cursor) #avisamos os usuários nacionais da realização da carga
    avisaUsuariosRegionais(conn) #avisamos usuários regionais dos TDPFs vincendos em curto prazo (pedido da Débora Difis07)
    cursor.close()
    conn.close() 
    return

def avisaUsuariosNacionais(texto, cursor): #avisamos os usuários nacionais do resultado da carga
    AMBIENTE = os.getenv("AMBIENTE", "TESTE") 
    texto = texto + "\n\nAmbiente: "+AMBIENTE
    consultaUNacionais = "Select email from Usuarios Inner Join Orgaos On Usuarios.Orgao=Orgaos.Codigo Where Usuarios.Orgao Is Not Null and Orgaos.Tipo='N'" 
    cursor.execute(consultaUNacionais)   
    rows = cursor.fetchall()
    #avisamos todos os usuários nacionais que houve uma carga de dados
    for row in rows:
        if enviaEmail(row[0], texto,"Carga de Dados Alertas Fiscalização")!=3: #se der erro
            logging.info("Erro no envio do e-mail para usuário NACIONAL "+row[0])   
            print("Erro no envio do e-mail para usuário NACIONAL "+row[0]) 
    return

def avisaUsuariosRegionais(conn): #avisamos usuários regionais dos TDPFs vincendos em curto prazo (4 a 10 dias - vai apenas um aviso, portanto, pois a carga é semanal)
    AMBIENTE = os.getenv("AMBIENTE", "TESTE") 
    if AMBIENTE!="PRODUÇÃO":
        return
    cursor = conn.cursor(buffered=True)
    consultaULocais = """Select email, Usuarios.Orgao from Usuarios, Orgaos 
                         Where Usuarios.Orgao Is Not Null and Usuarios.Orgao<>0 and Usuarios.Orgao=Orgaos.Codigo and email Is Not Null and Orgaos.Tipo='R' Order by email"""
    cursor.execute(consultaULocais)
    rows = cursor.fetchall()
    consultaTdpfs = """
                  Select Distinctrow TDPFS.Codigo, TDPFS.Numero, TDPFS.Grupo, Fiscais.Nome, TDPFS.Vencimento from Orgaos, TDPFS, Jurisdicao, Supervisores, Fiscais
                  Where TDPFS.Encerramento Is Null and Jurisdicao.Orgao=%s and Jurisdicao.Equipe=TDPFS.Grupo and Supervisores.Equipe=Jurisdicao.Equipe 
                  and Supervisores.Fim Is Null and Fiscais.Codigo=Supervisores.Fiscal and 
                  (TDPFS.Vencimento>=cast((now() + interval 4 day) as date) and TDPFS.Vencimento<=cast((now() + interval 10 day) as date)) 
                  Order by TDPFS.Grupo, TDPFS.Numero"""  #TDPFs que vencem de 4 a 10 dias (avisamos apenas uma vez, pois na próxima carga estará vencido, faltará menos de 4 dias ou terá sido renovado)
    consultaAviso = "Select Codigo, Data from AvisosVencimentoDifis Where TDPF=%s"    
    insere = set()
    atualiza = set()              
    for row in rows:
        total = 0
        email = row[0]
        cursor.execute(consultaTdpfs, (row[1], ))
        tdpfs = cursor.fetchall()
        equipeAnt = ""
        texto = ""
        i = 0
        for tdpf in tdpfs:
            chaveTdpf = tdpf[0]
            numero = tdpf[1]            
            cursor.execute(consultaAviso, (chaveTdpf,))
            aviso = cursor.fetchone()
            if aviso:
                if (aviso[1].date()+timedelta(days=6))>=datetime.now().date(): #não se passaram 7 dias desde o último aviso
                    continue #desprezamos este TDPF, pois já foi avisado há menos de uma semana
                else:
                    atualiza.add(aviso[0])
            else:
                insere.add(chaveTdpf)
            equipe = tdpf[2].strip()
            nome = tdpf[3]
            vencimento = tdpf[4]
            if equipeAnt!=equipe:
                equipeAnt = equipe
                if texto!="":
                    texto += "\n"
                texto += "Equipe: "+equipe[:7]+"."+equipe[7:11]+equipe[11:]+" - Supervisor: "+nome+"\n"
                i = 0
            total+=1
            i+=1
            texto = texto +"  "+str(i)+") "+numero[:7]+"."+numero[7:11]+"."+numero[11:]+" - "+vencimento.strftime("%d/%m/%Y")+"\n"
        if texto!="":
            texto = "Sr. Usuário Regional,\n\nEstamos enviando abaixo a relação de TDPFs de sua região com vencimento entre 4 e 10 dias:\n\n" + texto
            texto += "\nTotal de TDPFs: "+str(total)+"\n"
            texto += "\nDevido a restrições do DW, o vencimento no Ação Fiscal pode estar um pouco mais distante. Ressaltamos também que a carga de dados neste serviço ocorre semanalmente."
            texto += "\n\nAtenciosamente,\n\nCofis/Disav"
            if enviaEmail(email, texto, "TDPFs Vincendos Entre 4 e 10 Dias - Região")!=3:
                print("Erro no envio do e-mail para usuário REGIONAL "+email)
                logging.info("Erro no envio do e-mail para usuário REGIONAL "+email) 
    insereTuplas = []
    atualizaTuplas = []
    for chaveTdpf in insere:
        insereTuplas.append((chaveTdpf, datetime.now().date()))
    for codigo in atualiza:
        atualizaTuplas.append((datetime.now().date(), codigo))
    try:        
        cursor.executemany("Update AvisosVencimentoDifis Set Data=%s Where Codigo=%s", atualizaTuplas)
        cursor.executemany("Insert Into AvisosVencimentoDifis (TDPF, Data) Values (%s, %s)", insereTuplas)
        conn.commit()
        logging.info("Tabela AvisosVencimentoDifis foi atualizada com sucesso.")
    except:
        conn.rollback()
        logging.info("Atualização da tabela AvisosVencimentoDifis FALHOU!")
    return


def realizaCargaDCCs():
    global dirExcel, termina, hostSrv, hora2
    try:
        dfDCCs= pd.read_excel(dirExcel+"DCCS.xlsx", dtype={'DCC':object, 'Data':object})
    except:
        print("Erro no acesso ao arquivo de DCCS.xlsx ou só há arquivos já processados; outra tentativa será feita às "+hora2)
        logging.info("Arquivo Excel de DCCs não foi encontrado; outra tentativa será feita às "+hora2) 
        return
    dfDCCs['DCC']=dfDCCs['DCC'].astype(str)
    dfDCCs['Data']=dfDCCs['Data'].astype(str)
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234") 

    try:
        logging.info("Conectando ao servidor de banco de dados (2)...")
        logging.info(MYSQL_DATABASE)
        logging.info(MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv,
                                    database=MYSQL_DATABASE)
        logging.info("Conexão efetuada com sucesso ao MySql (2)!")                               
    except mysql.connector.Error as err:
        print("Erro na conexão com o BD (2) - veja Log: "+datetime.now().strftime('%d/%m/%Y %H:%M'))
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Usuário ou senha inválido(s) (2).")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Banco de dados não existe (2).")
        else:
            logging.error(err)
            logging.error("Erro na conexão com o Banco de Dados (2)")
        return
    print("Realizando carga dos DCCs em "+datetime.now().strftime("%d/%m/%Y %H:%M"))
    cursor = conn.cursor(buffered=True)   
    atualizou = False 
    for linha in range(dfDCCs.shape[0]):
        dcc = dfDCCs.iat[linha,0]        
        dataJuntada = dfDCCs.iat[linha,1]
        if dataJuntada=="HOJE":
            data = datetime.now()
        elif dataJuntada=="NULO":
            data = None
        else:
            data = paraData(dataJuntada)        
        comando = "Select Codigo From TDPFS Where DCC=%s"
        cursor.execute(comando, (dcc,))
        row = cursor.fetchone()
        if not row:
            continue
        if row[0]==None:
            continue
        tdpf = row[0]
        comando = "Select Codigo, Solicitacao from Juntadas Where TDPF=%s"
        cursor.execute(comando, (tdpf,))
        row = cursor.fetchone()
        if row==None:
            comando = "Insert Into Juntadas (TDPF, Solicitacao) Values (%s, %s)"
            cursor.execute(comando, (tdpf, data))
            atualizou = True
        else:
            codigo = row[0]
            ultJuntada = row[1]
            comando = "Update Juntadas Set Solicitacao=%s Where Codigo=%s"
            if ultJuntada==None and data!=None:
                cursor.execute(comando, (data, codigo))
                atualizou = True
            elif ultJuntada!=None and dataJuntada=="HOJE":
                pass
            elif ultJuntada==None and data==None:
                pass
            elif ultJuntada!=None and data==None:
                cursor.execute(comando, (data, codigo))
                atualizou = True                
            elif ultJuntada.date()<data.date():
                cursor.execute(comando, (data, codigo))
                atualizou = True
    bErro = False
    if atualizou:
        try:
            conn.commit()
            logging.info("Tabela de Juntadas atualizada")             
        except:
            conn.rollback()
            logging.error("Erro ao tentar atualizar a tabela de Juntadas")
            logging.error(mysql.connector.Error)
            print(mysql.connector.Error)              
            bErro = True
    if not bErro:
        try:
            os.rename(dirExcel+"DCCS.xlsx", dirExcel+"DCCS_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
            logging.info("Arquivo de DCCs renomeado")
        except:
            print("Erro ao tentar renomear o arquivo de DCCs")         
            logging.error("Erro ao tentar renomear o arquivo de DCCs")         
    print("Carga das Juntadas dos DCCs finalizada - ", datetime.now())
    conn.close()
    return

def realizaCargaCienciasPendentes():
    global dirExcel, termina, hostSrv, hora3
    try:
        dfCiencias= pd.read_excel(dirExcel+"CIENCIASPENDENTES.xlsx", 
                                  dtype={'RPF Gerencial - Num.':str, 'Proc. Doc. Lanç. - Numero Inf.': str, 
                                         'Dia Registro Doc. Lançamento':datetime, 'Data Última Extração': datetime})
    except:
        print("Erro no acesso ao arquivo de CIENCIASPENDENTES.xlsx ou só há arquivos já processados; outra tentativa será feita às "+hora3)
        logging.info("Arquivo Excel de CIENCIASPENDENTES não foi encontrado. Outra ciência será tentada às "+hora3) 
        return
    #dfCiencias['Proc. Doc. Lanç. - Numero Inf.']=dfCiencias['Proc. Doc. Lanç. - Numero Inf.'].astype(str)
    print(dfCiencias.dtypes)
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234") 

    try:
        logging.info("Conectando ao servidor de banco de dados (3)...")
        logging.info(MYSQL_DATABASE)
        logging.info(MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv,
                                    database=MYSQL_DATABASE)
        logging.info("Conexão efetuada com sucesso ao MySql (3)!")                               
    except mysql.connector.Error as err:
        print("Erro na conexão com o BD (2) - veja Log: "+datetime.now().strftime('%d/%m/%Y %H:%M'))
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Usuário ou senha inválido(s) (3).")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Banco de dados não existe (3).")
        else:
            logging.error(err)
            logging.error("Erro na conexão com o Banco de Dados (3)")
        return
    print("Realizando carga das Ciências Pendentes em "+datetime.now().strftime("%d/%m/%Y %H:%M"))   
    cursor = conn.cursor(buffered=True)   
    for linha in range(dfCiencias.shape[0]):   
        tdpf = getAlgarismos(dfCiencias.iat[linha, 0])
        processo = dfCiencias.iat[linha, 2]
        registro = dfCiencias.iat[linha, 6]
        extracao = dfCiencias.iat[linha, 10]  
        #print(extracao, " - ", print(paraData(extracao)))
        meio = dfCiencias.iat[linha, 9] #se o meio estiver preenchido, foi feita a ciência no SIEF Processos
        consultaTdpf = "Select Codigo from TDPFS Where Numero=%s"
        cursor.execute(consultaTdpf, (tdpf,))
        row = cursor.fetchone()
        if not row: #TDPF não encontrado - não interessa
            continue
        chaveTdpf = row[0]
        consultaProcesso = "Select Codigo, Finalizado from AvisosCiencia Where TDPF=%s and Processo=%s"
        cursor.execute(consultaProcesso, (chaveTdpf, processo))
        row = cursor.fetchone()
        if not row and meio in [None, np.nan, ""]: #registro não existe - devemos incluí-lo se meio não estiver preenchido (se meio estiver preenchido, houve ciência)
            print(chaveTdpf, " - ", processo)
            insere = "Insert Into AvisosCiencia (TDPF, Processo, Integracao, Extracao) Values (%s, %s, %s, %s)"
            try:
                cursor.execute(insere, (chaveTdpf, processo, paraData(registro), paraData(extracao)))
            except:
                print("Processo: " + processo + " - TDPF: "+tdpf)
                logging.error(mysql.connector.Error)
                logging.info("Processo: "+ processo + " - TDPF: "+tdpf)
                print(mysql.connector.Error)
        elif row and not meio in  [None, np.nan, ""]: #meio está preenchido - temos que finalizar o registro existente
            finalizaSQL = "Update AvisosCiencia Set Finalizado=%s, Extracao=%s Where Codigo=%s"
            cursor.execute(finalizaSQL, (datetime.now(), paraData(extracao), row[0]))
        elif row:
            extracaoSQL = "Update AvisosCiencia Set Extracao=%s Where Codigo=%s" #atualizamos a data de extração (a ciência ainda não foi registrada)
            cursor.execute(extracaoSQL, (paraData(extracao), row[0]))
    cursor.execute("Select MAX(Extracao) from AvisosCiencia")
    row = cursor.fetchone() #buscamos a maior data de extração registrada na tabela
    if row:
        extracao = row[0]
        #print("Extração (Max): ", extracao)
        #finalizamos todos os registros cuja extração se deu em data anterior à última extração (não foram atualizados acima)
        cursor.execute("Update AvisosCiencia Set Finalizado=%s Where Extracao<%s", (datetime.now(), extracao.date()))
    try:
        conn.commit()
        logging.error("Tabela de AvisosCiencia foi atualizada.")
        print("Tabela de AvisosCiencia foi atualizada - ", datetime.now())
        try:
            os.rename(dirExcel+"CIENCIASPENDENTES.xlsx", dirExcel+"CIENCIASPENDENTES_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
            logging.info("Arquivo de CIENCIASPENDENTES renomeado")
        except:
            print("Erro ao renomear arquivo de Ciências Pendentes")
            logging.error("Erro ao tentar renomear o arquivo de CIENCIASPENDENTES")                     
    except:
        conn.rollback()
        logging.error("Erro ao tentar atualizar a tabela de AvisosCiencia")
        print("Erro ao tentar atualizar a tabela de AvisosCiencia - ", datetime.now())
        logging.error(mysql.connector.Error)
        print(mysql.connector.Error)         
    return

def realizaCargaIndicadores(): #carga dos indicadores obtidos do Ação Fiscal na tabela Resultados
    global dirExcel, termina, hostSrv, hora4
    try:
        dfIndicadores= pd.read_excel(dirExcel+"INDICADORES.xlsx", dtype={'TDPF':object})
    except:
        print("Erro no acesso ao arquivo de INDICADORES.xlsx ou só há arquivos já processados; outra tentativa será feita às "+hora4)
        logging.info("Arquivo Excel de Indicadores não foi encontrado; outra tentativa será feita às "+hora4) 
        return
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "databasenormal")
    MYSQL_USER = os.getenv("MYSQL_USER", "my_user")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "mypass1234") 
    print(dfIndicadores.dtypes)
    try:
        logging.info("Conectando ao servidor de banco de dados (2)...")
        logging.info(MYSQL_DATABASE)
        logging.info(MYSQL_USER)

        conn = mysql.connector.connect(user=MYSQL_USER, password=MYSQL_PASSWORD,
                                    host=hostSrv, database=MYSQL_DATABASE)
        logging.info("Conexão efetuada com sucesso ao MySql (4)!")                               
    except mysql.connector.Error as err:
        print("Erro na conexão com o BD (4) - veja Log: "+datetime.now().strftime('%d/%m/%Y %H:%M'))
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Usuário ou senha inválido(s) (4).")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error("Banco de dados não existe (4).")
        else:
            logging.error(err)
            logging.error("Erro na conexão com o Banco de Dados (4)")
        return
    print("Realizando carga dos Indicadores em "+datetime.now().strftime("%d/%m/%Y %H:%M"))
    cursor = conn.cursor(buffered=True)   
    atualizou = False 
    consultaTdpf = "Select Codigo from TDPFS Where Numero=%s"
    consultaResultado = """Select Arrolamentos, MedCautelar, RepPenais, Inaptidoes, Baixas, ExcSimples, DigVincs from Resultados 
                           Where Resultados.TDPF=%s"""
    insere = "Insert Into Resultados (TDPF, Arrolamentos, MedCautelar, RepPenais, Inaptidoes, Baixas, ExcSimples, DigVincs, Data, SujPassivos, Situacao11, Interposicao, Situacao15, EstabPrev1, EstabPrev2, Segurados, Prestadores, Tomadores, QtdePER, LancMuldi, Compensacao, CreditoExt) Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 'N', 'N', 'N', 0, 0, 0, 0, 0, 0, 'N', 'N', 'N')"
    atualiza = "Update Resultados Set Arrolamentos=%s, MedCautelar=%s, RepPenais=%s, Inaptidoes=%s, Baixas=%s, ExcSimples=%s, DigVincs=%s Where TDPF=%s"
    incluidos = 0
    atualizados = 0
    for linha in range(dfIndicadores.shape[0]):
        if (linha+1)%300==0:
            print(linha+1, " processadas ...")
        tdpf = getAlgarismos(dfIndicadores.iat[linha, 0])
        cursor.execute(consultaTdpf, (tdpf,))
        row = cursor.fetchone()
        if row==None:
            #TDPF não encontrado - não há o que fazer
            continue
        chaveTdpf = row[0]
        parametros = []
        parametros.append(chaveTdpf)
        for i in range(1, 8, 1): #carrega os indicadores nesta lista
            parametros.append(int(dfIndicadores.iat[linha, i]))
            if i==2: #medida cautelar é sim (S) ou não (N)
                parametros[2] = "S" if dfIndicadores.iat[linha, 2]>0 else "N"
                
        #print(tdpf, parametros)
        cursor.execute(consultaResultado, (chaveTdpf,))
        row = cursor.fetchone()
        if row==None:
            parametros.append(datetime.now())
            cursor.execute(insere, tuple(parametros)) #não há a linha na tabela para o TDPF - fazemos sua inserção
            incluidos+=1
        else:
            parametros.pop(0)  #excluo a chaveTdpf da primeira posição para inclui-la na última, por conta da estrutura do SQL Update   
            #iria respeitar a informação outrora prestada, mas resolvi seguir a informação do Ação Fiscal (oficial), por isso comentei o for abaixo <--------
            #for i in range(7):
            #    if i==1:
            #        if parametros[1]=="N" and row[1]=="S": #medida cautelar é S ou N
            #            parametros[1] = "S" #se estava 'S', mantemos
            #    else:
            #        parametros[i] = max(parametros[i], row[i] if row[i]!=None else 0) #não reduzimos o valor já informado
            parametros.append(chaveTdpf)
            cursor.execute(atualiza, tuple(parametros))
            atualizados+=1
        atualizou = True
    print(linha+1, " processadas no total.")   
    print(incluidos, " registro incluídos.")
    print(atualizados, " registros atualizados.")         
    if atualizou:
        try:
            conn.commit()
            logging.error("Tabela de Resultados foi atualizada.")
            print("Tabela de Resultados foi atualizada - ", datetime.now())
            try:
                os.rename(dirExcel+"INDICADORES.xlsx", dirExcel+"INDICADORES_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
                logging.info("Arquivo de INDICADORES renomeado")
            except:
                print("Erro ao renomear o arquivo de Indicadores")
                logging.error("Erro ao tentar renomear o arquivo de INDICADORES")             
        except:
            conn.rollback()
            logging.error("Erro ao tentar atualizar a tabela de Resultados")
            print("Erro ao tentar atualizar a tabela de Resultados - ", datetime.now())
            logging.error(mysql.connector.Error)
            print(mysql.connector.Error)  
    else:
        print("Nenhuma atualização de indicadores ocorreu - ", datetime.now())
        logging.info("Nenhuma atualização de indicadores ocorreu.")
    return          


def disparador():
    global termina
    while not termina:
        schedule.run_pending() 
        time.sleep(60*60) #a cada hora, vê o que tem de tarefa pendente
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
hora1 = "09:30"
schedule.every().day.at(hora1).do(realizaCargaDados) #a cada 24 horas, verifica se há arquivos para fazer a carga
hora2 = "14:30"
schedule.every().day.at(hora2).do(realizaCargaDCCs)
hora3 = "11:30"
schedule.every().day.at(hora3).do(realizaCargaCienciasPendentes)
hora4 = "10:30"
schedule.every().day.at(hora4).do(realizaCargaIndicadores)
termina = False
threadDisparador = threading.Thread(target=disparador, daemon=True) #encerra thread quando sair do programa sem esperá-la
threadDisparador.start()
realizaCargaDados() #faz a primeira tentativa de carga das planilhas logo no acionamento do programa
realizaCargaDCCs() #idem para os DCCs
realizaCargaCienciasPendentes() #ciencias pendentes de AI 
realizaCargaIndicadores() #carga de indicadores (parâmetros) dos TDPFs encerrados
while not termina:
    entrada = input("Digite QUIT para terminar o serviço Carga BOT: ")
    if entrada:
        if entrada.strip().upper()=="QUIT":
            termina = True
schedule.clear()        