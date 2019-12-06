DROP TABLE IF EXISTS power;
CREATE TABLE power (
  bob    DATE     NOT NULL,
  eob    DATE     NULL,
  symbol CHAR(11) NOT NULL,
  bpol   VARCHAR(8)  NOT NULL,
  spol   VARCHAR(8)  NULL,
  PRIMARY KEY (bob, symbol)
);