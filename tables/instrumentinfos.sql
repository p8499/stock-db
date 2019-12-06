DROP TABLE IF EXISTS instrumentinfos;
CREATE TABLE instrumentinfos (
  symbol        CHAR(11)       NOT NULL,
  sec_name      VARCHAR(64)    NOT NULL,
  sec_abbr      VARCHAR(64)    NOT NULL,
  price_tick    NUMERIC(32, 8) NOT NULL,
  listed_date   DATE           NOT NULL,
  delisted_date DATE           NOT NULL,
  PRIMARY KEY (symbol)
);