DROP TABLE IF EXISTS history_1d;
CREATE TABLE history_1d (
  symbol CHAR(11)       NOT NULL,
  bob    DATE           NOT NULL,
  eob    DATE           NOT NULL,
  open   NUMERIC(32, 8) NOT NULL,
  close  NUMERIC(32, 8) NOT NULL,
  high   NUMERIC(32, 8) NOT NULL,
  low    NUMERIC(32, 8) NOT NULL,
  amount NUMERIC(32, 8) NOT NULL,
  volume NUMERIC(32, 8) NOT NULL,
  PRIMARY KEY (symbol, bob, eob)
);