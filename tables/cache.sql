DROP TABLE IF EXISTS cache;
CREATE TABLE cache (
  symbol CHAR(11)       NOT NULL,
  date   DATE           NOT NULL,
  kpi    VARCHAR(16)    NOT NULL,
  val    NUMERIC(32, 8) NULL,
  PRIMARY KEY (symbol, date, kpi)
);