DROP TABLE IF EXISTS history_instruments;
CREATE TABLE history_instruments (
  symbol       CHAR(11)       NOT NULL,
  trade_date   DATE           NOT NULL,
  sec_level    INT            NOT NULL,
  is_suspended INT            NOT NULL,
  multiplier   NUMERIC(32, 8) NOT NULL,
  margin_ratio NUMERIC(32, 8) NOT NULL,
  settle_price NUMERIC(32, 8) NOT NULL,
  pre_settle   NUMERIC(32, 8) NOT NULL,
  position     NUMERIC(32, 8) NOT NULL,
  pre_close    NUMERIC(32, 8) NOT NULL,
  upper_limit  NUMERIC(32, 8) NOT NULL,
  lower_limit  NUMERIC(32, 8) NOT NULL,
  adj_factor   NUMERIC(32, 8) NOT NULL,
  PRIMARY KEY (symbol, trade_date)
);