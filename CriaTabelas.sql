DROP TABLE IF EXISTS `Alocacoes`;

CREATE TABLE `Alocacoes` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Fiscal` INTEGER, 
  `TDPF` BIGINT, 
  `Alocacao` DATETIME, 
  `Desalocacao` DATETIME, 
  `Supervisor` VARCHAR(1), 
  `Horas` INTEGER DEFAULT 0,
  INDEX (`Fiscal`), 
  INDEX (`Fiscal`, `Supervisor`), 
  INDEX (`Fiscal`, `TDPF`), 
  PRIMARY KEY (`Codigo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `CadastroTDPFs`;

CREATE TABLE `CadastroTDPFs` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Fiscal` INTEGER, 
  `TDPF` BIGINT, 
  `Inicio` DATETIME, 
  `Fim` DATETIME, 
  INDEX (`Fiscal`), 
  UNIQUE (`Fiscal`, `TDPF`), 
  PRIMARY KEY (`Codigo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Ciencias`;

CREATE TABLE `Ciencias` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT, 
  `Data` DATETIME, 
  `Documento` VARCHAR(70),
  `Vencimento` DATETIME,
  PRIMARY KEY (`Codigo`), 
  INDEX (`TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Fiscais`;

CREATE TABLE `Fiscais` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `CPF` VARCHAR(11), 
  `Nome` VARCHAR(255), 
  `Matricula` VARCHAR(20), 
  UNIQUE (`CPF`), 
  PRIMARY KEY (`Codigo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `TDPFS`;

CREATE TABLE `TDPFS` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Numero` VARCHAR(16), 
  `Grupo` VARCHAR(25), 
  `Emissao` DATETIME, 
  `Encerramento` DATETIME, 
  `Nome` VARCHAR(150), 
  `NI` VARCHAR(18), 
  `Vencimento` DATETIME, 
  `DCC` CHAR(17), 
  `Porte` VARCHAR(50),
  `Acompanhamento` VARCHAR(1),
  `TrimestrePrevisto` VARCHAR(6),
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`Numero`), 
  UNIQUE (`Numero`, `Grupo`),
  UNIQUE (`DCC`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Usuarios`;

CREATE TABLE `Usuarios` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `CPF` VARCHAR(11), 
  `idTelegram` INTEGER DEFAULT 0, 
  `Adesao` DATETIME, 
  `Saida` DATETIME, 
  `d1` INTEGER DEFAULT 0, 
  `d2` INTEGER DEFAULT 0, 
  `d3` INTEGER DEFAULT 0, 
  `email` VARCHAR(100), 
  `Chave` INTEGER DEFAULT 0, 
  `ValidadeChave` DATETIME, 
  `Tentativas` INTEGER DEFAULT 0, 
  `DataEnvio` DATETIME,
  `Orgao` INTEGER DEFAULT 0,
  INDEX (`CPF`), 
  INDEX (`idTelegram`), 
  PRIMARY KEY (`Codigo`), 
  INDEX (`ValidadeChave`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Atividades`;

CREATE TABLE `Atividades` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT,
  `TDPF` BIGINT, 
  `Atividade` VARCHAR(50), 
  `Vencimento` DATETIME, 
  `Termino` DATETIME,
  `Inicio` DATETIME,
  `Horas` INTEGER DEFAULT 0,
  `Observacoes` VARCHAR(100),
  PRIMARY KEY (`Codigo`), 
  INDEX (`TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AvisosVencimento`;

CREATE TABLE `AvisosVencimento` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT, 
  `Fiscal` INTEGER, 
  `Data` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`TDPF`),
  INDEX (`Fiscal`),
  UNIQUE (`TDPF`, `Fiscal`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `MensagensCofis`;

CREATE TABLE `MensagensCofis` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Mensagem` VARCHAR(200), 
  `Data` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`Data`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AvisosUrgentes`;

CREATE TABLE `AvisosUrgentes` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Mensagem` VARCHAR(200), 
  `DataEnvio` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`DataEnvio`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Supervisores`;

CREATE TABLE `Supervisores` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Equipe` VARCHAR(14), 
  `Fiscal` INTEGER,
  `Inicio` DATETIME, 
  `Fim` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`Fiscal`),
  INDEX (`Equipe`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `DiarioFiscalizacao`;

CREATE TABLE `DiarioFiscalizacao` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Fiscal` INTEGER,
  `TDPF` BIGINT,
  `Data` DATETIME, 
  `Entrada` VARBINARY(16384),  
  PRIMARY KEY (`Codigo`), 
  INDEX (`Fiscal`),
  INDEX (`TDPF`),
  INDEX (`Fiscal`, `TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Resultados`;

CREATE TABLE `Resultados` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT,
  `Arrolamentos` INTEGER,
  `MedCautelar` VARCHAR(1),
  `RepPenais` INTEGER,
  `Inaptidoes` INTEGER,
  `Baixas` INTEGER,
  `ExcSimples` INTEGER,
  `SujPassivos` INTEGER,
  `DigVincs` INTEGER,
  `Situacao11` VARCHAR(1),
  `Interposicao` VARCHAR(1),
  `Situacao15` VARCHAR(1),
  `EstabPrev1` INTEGER,
  `EstabPrev2` INTEGER,  
  `Segurados` INTEGER,
  `Prestadores` INTEGER,
  `Tomadores` INTEGER,
  `QtdePER` INTEGER,
  `LancMuldi` VARCHAR(1),
  `Compensacao` VARCHAR(1),
  `CreditoExt` VARCHAR(1),
  `Data` DATETIME,  
  `CPF` VARCHAR(11),
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Operacoes`;

CREATE TABLE `Operacoes` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT,
  `Operacao` BIGINT,
  `PeriodoInicial` DATETIME, 
  `PeriodoFinal` DATETIME,
  `Tributo` INTEGER,
  PRIMARY KEY (`Codigo`), 
  INDEX (`TDPF`),
  UNIQUE (`TDPF`, `Operacao`, `Tributo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;  

DROP TABLE IF EXISTS `OperacoesFiscais`;

CREATE TABLE `OperacoesFiscais` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Operacao` INTEGER,
  `Descricao` VARCHAR(200),
  `Valor` DECIMAL(3,2),
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`Operacao`)
) ENGINE=innodb DEFAULT CHARSET=utf8; 

DROP TABLE IF EXISTS `Tributos`;

CREATE TABLE `Tributos` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Tributo` INTEGER,
  `Descricao` VARCHAR(200),
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`Tributo`)
) ENGINE=innodb DEFAULT CHARSET=utf8; 

DROP TABLE IF EXISTS `Log`;

CREATE TABLE `Log` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `IP` VARCHAR(15),
  `Requisicao` INTEGER,
  `Mensagem` VARCHAR(250),
  `Data` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  Index (`IP`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Orgaos`;

CREATE TABLE `Orgaos` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Orgao` VARCHAR(25),
  `Tipo` CHAR(1) DEFAULT 'L',
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`Orgao`)
) ENGINE=innodb DEFAULT CHARSET=utf8; 

DROP TABLE IF EXISTS `Jurisdicao`;

CREATE TABLE `Jurisdicao` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Orgao` INTEGER,
  `Equipe` VARCHAR(25),
  PRIMARY KEY (`Codigo`), 
  INDEX (`Orgao`),
  INDEX (`Equipe`)
) ENGINE=innodb DEFAULT CHARSET=utf8; 

DROP TABLE IF EXISTS `Juntadas`;

CREATE TABLE `Juntadas` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT,  
  `TDPF` BIGINT, 
  `Solicitacao` DATETIME, 
  `Aviso` DATETIME, 
  INDEX (`TDPF`),  
  PRIMARY KEY (`Codigo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Prorrogacoes`;

CREATE TABLE `Prorrogacoes` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT, 
  `Assunto` VARCHAR(100),
  `Documento` VARCHAR(100),
  `Tipo` VARCHAR(2),  
  `Data` DATETIME, 
  `Supervisor` INTEGER,
  `DataAssinatura` DATETIME, 
  `Fundamentos` VARCHAR(2000),
  `Numero` INTEGER,
  `Motivo` INTEGER,
  `RegistroRHAF` DATETIME,
  PRIMARY KEY (`Codigo`), 
  INDEX (`TDPF`),
  UNIQUE (`TDPF`, `Data`),
  UNIQUE (`TDPF`, `Numero`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AssinaturaFiscal`;

CREATE TABLE `AssinaturaFiscal` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `Prorrogacao` BIGINT, 
  `Fiscal` INTEGER,
  `DataAssinatura` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`Prorrogacao`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AvisosCiencia`;

CREATE TABLE `AvisosCiencia` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT, 
  `Processo` VARCHAR(17),
  `Integracao` DATETIME,
  `Extracao` DATETIME,
  `Aviso` DATETIME, 
  `Finalizado` DATETIME,
  PRIMARY KEY (`Codigo`),   
  INDEX (`TDPF`), 
  UNIQUE (`TDPF`, `Processo`), 
  UNIQUE (`Processo`) 
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AvisosVencimentoDifis`;

CREATE TABLE `AvisosVencimentoDifis` (
  `Codigo` BIGINT NOT NULL AUTO_INCREMENT, 
  `TDPF` BIGINT, 
  `Data` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  UNIQUE (`TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;