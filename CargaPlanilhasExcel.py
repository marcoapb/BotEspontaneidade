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
    global dirExcel, termina, hostSrv
    try:
        dfTdpf = pd.read_excel(dirExcel+"TDPFS.xlsx")
        dfAloc = pd.read_excel(dirExcel+"ALOCACOES.xlsx")
        dfFiscais = pd.read_excel(dirExcel+"Fiscais.xlsx")
        dfSupervisores = pd.read_csv(dirExcel+"Supervisores.CSV", sep=";", encoding = "ISO-8859-1")
    except:
        print("Erro no acesso aos arquivos xlsx e/ou csv")
        logging.info("Arquivos Excel não foram encontrados (um ou mais) - TDPFs.xlsx, Alocacoes.xlsx, Fiscais.xlsx ou Supervisores.CSV; outra tentativa será feita em 24h") 
        return
    dfFiscais['CPF']=dfFiscais['CPF'].astype(str).map(acrescentaZeroCPF) 
    dfSupervisores['CPF']=dfSupervisores['R028_RH_PF_NR'].astype(str).map(calculaDVCPF)
    dfSupervisores['Grupo Fiscal']=dfSupervisores.apply(montaGrupoFiscal, axis=1)
    MYSQL_ROOT_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", "EXAMPLE")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "testedb")
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

    selectFisc = "Select * from Fiscais Where CPF=%s"
    insereFisc = "Insert Into Fiscais (CPF, Nome) Values (%s, %s)"

    selectTDPF = "Select Codigo, Grupo, Encerramento, Vencimento from TDPFS Where Numero=%s"
    insereTDPF = "Insert Into TDPFS (Numero, Grupo, Emissao, Nome, NI, Vencimento) Values (%s, %s, %s, %s, %s, %s)"
    atualizaTDPFEnc = "Update TDPFS Set Encerramento=%s Where Codigo=%s"
    atualizaTDPFGrupoVencto = "Update TDPFS Set Grupo=%s, Vencimento=%s Where Codigo=%s"

    selectAloc = "Select Codigo, Desalocacao, Supervisor from Alocacoes Where TDPF=%s and CPF=%s"
    insereAloc = "Insert Into Alocacoes (TDPF, CPF, Alocacao, Supervisor, Horas) Values (%s, %s, %s, %s, %s)"
    insereAlocDesaloc = "Insert Into Alocacoes (TDPF, CPF, Alocacao, Desalocacao, Supervisor, Horas) Values (%s, %s, %s, %s, %s, %s)"
    atualizaAloc = "Update Alocacoes Set Desalocacao=%s, Supervisor=%s, Horas=%s Where Codigo=%s"
    atualizaAlocHoras = "Update Alocacoes Set Horas=%s Where Codigo=%s"

    selectCiencias = "Select * from Ciencias Where TDPF=%s" 
    insereCiencia = "Insert Into Ciencias (TDPF, Data, Documento) Values (%s, %s, %s)"

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
        distribuicao = dfTdpf.iloc[linha, 9] #na verdade, aqui é a data de assinatura/emissão do TDPF (antes tinha apenas a distribuição) 
                                              #<- a assinatura revelou-se pior que a distribuição - voltei a usar esta
        inicio = dfTdpf.iloc[linha, 11]
        encerramento = dfTdpf.iloc[linha, 12]
        #situacao = dfTdpf.iloc[linha, 13]
        ni = dfTdpf.iloc[linha, 17]
        nome = dfTdpf.iloc[linha, 18]
        cursor.execute(selectTDPF, (tdpf,))
        regTdpf = cursor.fetchone()    
        if not regTdpf and encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None:    #TDPF encerrado e não existente na base - pulamos
            continue
        if regTdpf:
            if regTdpf[2]!=None:
                continue #TDPF já encerrado na base - não há interesse em atualizar
        df = dfAloc.loc[dfAloc['Número do RPF Expresso']==tdpfAux]
        if df.shape[0]==0:
            logging.info(f"TDPFs: {tdpfAux} não tem fiscal alocado - TDPF foi desprezado.")
            continue
        grupoAtu = ""
        for linha2 in range(df.shape[0]):
            grupo = df.iat[linha2, 4]
            grupo = getAlgarismos(grupo)
            cpf = getAlgarismos(df.iat[linha2, 6])
            fiscal = df.iat[linha2, 7] #nome do fiscal
            alocacao = df.iat[linha2, 9]
            desalocacao = df.iat[linha2, 10]
            if desalocacao=="SD" or desalocacao=="" or paraData(desalocacao)==None:
                grupoAtu = grupo   
            supervisor = df.iat[linha2, 12][:1]
            horas = df.iat[linha2, 16]
            try:    
                horas = int(horas)    
            except:
                horas = 0    
            dfFiscal = dfFiscais.loc[dfFiscais['CPF']==cpf]
            email = None
            if dfFiscal.shape[0]>0:
                if dfFiscal.iat[0, 4]!=np.nan and dfFiscal.iat[0, 4]!="": #email está na coluna 4 (coluna 'E' do Excel)
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
            cursor.execute(selectAloc, (tdpf, cpf))
            regAloc = cursor.fetchone()
            if not regAloc:
                tabAloc+=1
                if desalocacao=="SD" or desalocacao=="" or paraData(desalocacao)==None:
                    atualizou = True
                    cursor.execute(insereAloc, (tdpf, cpf, paraData(alocacao), supervisor, horas))
                else:
                    atualizou = True
                    cursor.execute(insereAlocDesaloc, (tdpf, cpf, paraData(alocacao), paraData(desalocacao), supervisor, horas))
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
        if distribuicao:
            distData = paraData(distribuicao) 
            vencimento = distData + timedelta(days=(120-1))
            while vencimento.date()<datetime.now().date():
                vencimento = vencimento + timedelta(days=120)
                if encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None:
                    if vencimento.date()>paraData(encerramento).date(): #assim que passou do encerramento, paramos de acrescentar 120 dias ao vencimento
                        break
        else: #distribuição nulo? Não deve acontecer, mas ...
            vencimento = None   
            distData = None     
        if not regTdpf:
            tabTdpfs+=1
            atualizou = True
            cursor.execute(insereTDPF, (tdpf, grupoAtu, distData, nome, ni, vencimento))
            if inicio!="SD" and inicio!="" and paraData(inicio)!=None:
                tabCiencias+=1
                cursor.execute(insereCiencia, (tdpf, paraData(inicio), "ACÃO FISCAL"))
        elif regTdpf[2]==None and encerramento!="SD" and encerramento!="" and paraData(encerramento)!=None:
            tabTdpfsAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFEnc, (paraData(encerramento), regTdpf[0]))
        elif regTdpf[1]!=grupoAtu or regTdpf[3].date()<datetime.now().date(): #mudou o grupo e/ou TDPF está vencido, mas, em ambos os casos, NÃO encerrado - atualiza grupo e vencimento
            gruposAtu+=1
            atualizou = True
            cursor.execute(atualizaTDPFGrupoVencto, (grupoAtu, vencimento, regTdpf[0]))
        if regTdpf and inicio!="SD" and inicio!="" and paraData(inicio)!=None:
            cursor.execute(selectCiencias, (tdpf,))
            regCiencia = cursor.fetchone()
            if not regCiencia:
                tabCiencias+=1
                atualizou = True
                cursor.execute(insereCiencia, (tdpf, paraData(inicio), "ACÃO FISCAL"))
        if atualizou:        
            conn.commit() 
    #atualizamos a tabela de supervisões de grupos/equipes fiscais (Supervisores)
    comando = "Select Distinctrow Grupo from TDPFS"
    cursor.execute(comando)
    gruposRows = cursor.fetchall()
    tabGrupos = 0
    tabGruposAtu = 0
    atualizouSuperv = False
    superv = 0 #número de supervisores que não fazem parte de nenhum grupo - são incluídos na tabela de usuários pq supervisionam ativamente alguma equipe
    for grupoRow in gruposRows:
        df = dfSupervisores.loc[dfSupervisores['Grupo Fiscal']==grupoRow[0]].sort_values(by=['R028_DT_INI_VINCULO'], ascending=False)
        if df.shape[0]>0: #pega só o último registro da supervisão da equipe (mais recente início na supervisão)
            cpf = df.iat[0, 12] 
            dataIni = df.iat[0, 9]
            dataFim = df.iat[0, 10]
            if dataFim==None or dataFim=="" or np.isnan(dataFim) or dataFim==float(0): #é supervisor da equipe
                #print(grupoRow[0]+" - "+cpf)
                comando = "Select Codigo, CPF, Inicio, Fim from Supervisores Where Equipe=%s and CPF=%s and Fim Is Null"
                cursor.execute(comando, (grupoRow[0], cpf))
                supervisoresRows = cursor.fetchall()
                bAchou = True
                if supervisoresRows==None:
                    bAchou = False
                elif len(supervisoresRows)==0:
                    bAchou = False
                #print(bAchou)    
                if not bAchou: #ainda não consta da tabela de Supervisores
                    comando = "Insert Into Supervisores (Equipe, CPF, Inicio) Values (%s, %s, %s)"
                    tabGrupos+=1
                    cursor.execute(comando, (grupoRow[0], cpf, paraData(dataIni)))
                    #verificamos se este grupo não tem outro servidor ativo - se tiver, colocamos a data final - fazemos isso para garantir caso haja uma descontinuidade
                    #não obtida pelo else abaixo
                    comando = "Select Codigo from Supervisores Where Equipe=%s and CPF<>%s and Fim Is Not Null"
                    cursor.execute(comando, (grupoRow[0], cpf))                    
                    supervisoresRows = cursor.fetchall()
                    if supervisoresRows!=None:
                        if len(supervisoresRows)>0:
                            comando = "Update Supervisores Set Fim=%s Where Equipe=%s and CPF<>%s and Fim Is Not Null" #"matamos" os antigos supervisores
                            cursor.execute(comando, (datetime.now().date(), grupoRow[0], cpf))
                #verificamos se este supervisor consta da tabela de usuários e fiscais
                dfFiscal = dfFiscais.loc[dfFiscais['CPF']==cpf]
                email = None
                if dfFiscal.shape[0]>0: 
                    fiscal =  dfFiscal.iat[0, 2] #nome do fiscal
                    if dfFiscal.iat[0, 4]!=np.nan and dfFiscal.iat[0, 4]!="": #email está na coluna 4 (coluna 'E' do Excel)
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
                    cursor.execute(selectFisc, (cpf,))
                    regFisc = cursor.fetchone()
                    if not regFisc: #não existe o supervisor na tabela de fiscais
                        tabFiscais+=1
                        atualizouSuperv = True
                        cursor.execute(insereFisc, (cpf, fiscal.upper()))                                 
            else: #não é mais supervisor da equipe
                comando = "Select Codigo, CPF, Inicio, Fim from Supervisores Where Equipe=%s and CPF=%s and Inicio=%s and Fim Is Null"
                cursor.execute(comando, (grupoRow[0], cpf, paraData(dataIni)))
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

    if tabGrupos>0 or tabGruposAtu>0 or atualizouSuperv:
        conn.commit()
    cursor.close()
    conn.close()  

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
   
    print("Supervisores não alocados a TDPFs e incluídos: "+str(superv))
    try:
        os.rename(dirExcel+"TDPFS.xlsx", dirExcel+"TDPFS_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        os.rename(dirExcel+"ALOCACOES.xlsx", dirExcel+"ALOCACOES_Processado_"+datetime.now().strftime('%Y-%m-%d')+".xlsx")
        logging.info("Arquivos renomeados")
    except:
        logging.error("Erro ao tentar renomear os arquivos")
    return


def disparador():
    global termina
    while not termina:
        schedule.run_pending() 
        time.sleep(24*60*60) #espera 24 horas para tentar realizar outra carga        
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
schedule.every(24).hours.do(realizaCargaDados) #a cada 24 horas, verifica se há arquivos para fazer a carga
termina = False
threadDisparador = threading.Thread(target=disparador, daemon=True) #encerra thread quando sair do programa sem esperá-la
threadDisparador.start()
realizaCargaDados() #faz a primeira tentativa de carga das planilhas logo no acionamento do programa
while not termina:
    entrada = input("Digite QUIT para terminar o serviço Carga BOT: ")
    if entrada:
        if entrada.strip().upper()=="QUIT":
            termina = True
schedule.clear()        