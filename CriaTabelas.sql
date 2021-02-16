DROP TABLE IF EXISTS `Alocacoes`;

CREATE TABLE `Alocacoes` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Fiscal` INTEGER, 
  `TDPF` INTEGER, 
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
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Fiscal` INTEGER, 
  `TDPF` INTEGER, 
  `Inicio` DATETIME, 
  `Fim` DATETIME, 
  INDEX (`Fiscal`), 
  UNIQUE (`Fiscal`, `TDPF`), 
  PRIMARY KEY (`Codigo`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Ciencias`;

CREATE TABLE `Ciencias` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `TDPF` INTEGER, 
  `Data` DATETIME, 
  `Documento` VARCHAR(50),
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
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Numero` VARCHAR(16), 
  `Grupo` VARCHAR(25), 
  `Emissao` DATETIME, 
  `CodigoAcesso` INTEGER DEFAULT 0, 
  `Encerramento` DATETIME, 
  `Nome` VARCHAR(150), 
  `NI` VARCHAR(18), 
  `Vencimento` DATETIME, 
  `DCC` CHAR(17), 
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
  INDEX (`CPF`), 
  INDEX (`idTelegram`), 
  PRIMARY KEY (`Codigo`), 
  INDEX (`ValidadeChave`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `Atividades`;

CREATE TABLE `Atividades` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `TDPF` INTEGER, 
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
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `TDPF` INTEGER, 
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
  `Mensagem` VARCHAR(100), 
  `Data` DATETIME, 
  PRIMARY KEY (`Codigo`), 
  INDEX (`Data`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `AvisosUrgentes`;

CREATE TABLE `AvisosUrgentes` (
  `Codigo` INTEGER NOT NULL AUTO_INCREMENT, 
  `Mensagem` VARCHAR(100), 
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
  `Fiscal` BIGINT,
  `TDPF` BIGINT,
  `Data` DATETIME, 
  `Entrada` VARBINARY(16384),  
  PRIMARY KEY (`Codigo`), 
  INDEX (`Fiscal`),
  INDEX (`TDPF`),
  INDEX (`Fiscal`, `TDPF`)
) ENGINE=innodb DEFAULT CHARSET=utf8;

