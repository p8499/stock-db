DROP TABLE IF EXISTS industry_constituents;
CREATE TABLE industry_constituents (
  code   CHAR(3)  NOT NULL,
  symbol CHAR(11) NOT NULL,
  PRIMARY KEY (code, symbol)
);