DROP TABLE IF EXISTS finance;
CREATE TABLE cache(
  symbol CHAR(11)    NOT NULL,
  date   DATE        NOT NULL,
  kpi    VARCHAR(16) NOT NULL,
  val    NUMERIC(32, 8),
  PRIMARY KEY (symbol, date, kpi)
);

