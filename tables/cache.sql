DROP TABLE IF EXISTS finance;
CREATE TABLE finance (
  symbol CHAR(11) NOT NULL,
  date   DATE     NOT NULL,
  PRIMARY KEY (symbol, date)
);