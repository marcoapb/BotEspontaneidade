from __future__ import unicode_literals
from datetime import datetime, timedelta 
import socket
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import sys
import os

def geraChave(): #gera a chave (par) do usuário, coloca no dicionário de validades e retorna a chave pública para ser encaminhada a ele
    global chaveCripto
    keyPair = RSA.generate(2048)
    decryptor = PKCS1_OAEP.new(keyPair)        
    chaveCripto = [keyPair, decryptor, datetime.now()+timedelta(minutes=15)] #validade de 15 minutos para este conjunto de chaves
    #pubKey = keyPair.publickey()
    return #pubKey

def descriptografa(msgCripto):
    global chaveCripto
    try:
        decrypted = chaveCripto[1].decrypt(msgCripto)#.decode("utf-8")
        #decrypted = decryptor.decrypt(msgCripto)
        return True, decrypted
    except:
        return False, ""      

def servidorArquivo(): 
    global chaveCripto

    SENHAIMPORTACAO = os.getenv("SENHAIMPORTACAO", None)
    sistema = sys.platform.upper()
    if "WIN32" in sistema or "WIN64" in sistema or "WINDOWS" in sistema:
        dirExcel = 'Excel\\'
    else:
        dirExcel = '/Excel/'    

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)         
    print("Socket successfully created")     
    port = 80          
      
    # Next bind to the port 
    # we have not typed any ip in the ip field 
    # instead we have inputted an empty string 
    # this makes the server listen to requests  
    # coming from other computers on the network 
    s.bind(('', port))         
    print("socket binded to %s" %(port))
      
    # put the socket into listening mode 
    s.listen(5)     
    print("socket is listening")
      
    #só fica escutando a rede, pega a mensagem e encaminha para tratamento 
    while True:  
        c, addr = s.accept()      
        geraChave()
        print('Got connection from ' + str(addr))    
        c.settimeout(15)
        try:            
            msgRecebida = c.recv(1024).decode('utf-8')
            if msgRecebida=="00CHAVE":
                tam = str(len(chaveCripto[0].publickey().export_key())).rjust(5,"0").encode('utf-8')
                c.sendall(tam+chaveCripto[0].publickey().export_key()) #envia a chave pública (tamanho = 450)
            else:
                continue #nem responde (primeira requisição tem que ser da chave)
            msgRecebida = c.recv(2048)
            tamanho = int(msgRecebida[:5].decode('utf-8'))
            msgRecebida = msgRecebida[5:]
            if len(msgRecebida)<tamanho:
                msgRecebida = msgRecebida+c.recv(2048)
            sucesso, msgDescripto = descriptografa(msgRecebida)
            if sucesso:
                msgRecebida = msgDescripto.decode('utf-8')
                horaSenha = str(int(int(datetime.now().strftime('%H%M'))/10)).rjust(3, "0") #da hhmm, pega a hhm para complementar a senha
                print(SENHAIMPORTACAO+horaSenha)
                if msgRecebida!="ENVIAARQUIVOS"+SENHAIMPORTACAO+horaSenha and SENHAIMPORTACAO!=None:
                    print(msgRecebida)
                    print("Senha inválida ou requisição estranha")
                    c.close()
                    continue
            else:
                print("Erro na conexão - criptografia da mensagem recebida é desconhecida")
                c.close
                continue
            try:
                print("Recebeu requisição ...")
                resposta = "AGUARDANDOMAIS09876MAPB".encode('utf-8') 
                c.sendall(resposta)
            except:
                print("Erro ao enviar mensagem 1")
                c.close()
                continue             
            texto = b""
            nomeArq = ""
            bPrim = False
            buffer = 20480 #tamanho máximo de cada mensagem (a inicial, criptografada, só tem 400)           
            while True:
                if chaveCripto[2]<datetime.now():
                    print("Chave venceu ...")
                    break #chave venceu
                tam = 4
                msgRecebida = c.recv(buffer) 
                tam = int(msgRecebida[:5].decode("utf-8"))
                msgRecebida = msgRecebida[5:]
                tamEfetivo = len(msgRecebida)
                while tamEfetivo<tam:
                    msgRecebida = msgRecebida+c.recv(buffer) 
                    tamEfetivo = len(msgRecebida)
                if bPrim: #primeiro pedaço do arquivo vem criptografado
                    bPrim = False
                    sucesso, msgDescripto = descriptografa(msgRecebida) 
                    if sucesso:
                        texto = texto+msgDescripto 
                        resposta = "AGUARDANDOMAIS09876MAPB".encode('utf-8')   
                        try:
                            c.sendall(resposta)
                        except:
                            print("Erro ao enviar mensagem 4")
                            break                    
                        continue 
                    else:
                        print("Erro ao receber o primeiro pedaço do arquivo ...")
                        break             
                bCaptura = True
                sucesso, msgDescripto = descriptografa(msgRecebida)
                if sucesso:
                    try:
                        bCaptura = False                    
                        msgUtf =  msgDescripto.decode("utf-8")
                        #print(msgUtf)
                        if msgUtf[:25]=="ARQUIVONOME1234509876MAPB":
                            nomeArq = msgUtf[25:].strip()
                            if nomeArq in ["TDPFS", "ALOCACOES", "OPERACOES", "DCCS", "CIENCIASPENDENTES", "INDICADORES"]:
                                nomeArq = nomeArq+".xlsx"
                            else:
                                print("Nome de arquivo inválido ...")
                                break #arquivo de nome inválido
                            print("Recebendo arquivo "+nomeArq)
                            texto = b""
                            arq = open(dirExcel+nomeArq,'wb')
                            bPrim = True #iremos receber o primeiro pedaço do arquivo criptografado
                        elif msgUtf[:26]=="SALVAARQUIVO1234509876MAPB":
                            arq.write(texto)
                            print("Salvando arquivo "+nomeArq)
                            arq.close()
                        elif msgUtf[:28]=="FINALIZATRANSM1234509876MAPB":
                            print("Envio finalizado")
                            break
                        else:
                            pass
                    except:
                        pass                
                if bCaptura:
                    texto = texto + msgRecebida #(pedaços do arquivo vem descriptografados, exceto o primeiro)
                    #print("Recebeu um pedaço - "+nomeArq)
                resposta = "AGUARDANDOMAIS09876MAPB".encode('utf-8')   
                try:
                    c.sendall(resposta)
                except:
                    print("Erro ao enviar mensagem 2")
                    break
        except:
            print("Erro na conexão - time out - cliente não enviou mensagem")
        c.close()



servidorArquivo()
