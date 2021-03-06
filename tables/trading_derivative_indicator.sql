DROP TABLE IF EXISTS trading_derivative_indicator;
CREATE TABLE trading_derivative_indicator (
  symbol       CHAR(11)       NOT NULL,
  pub          DATE           NOT NULL,
  "end"        DATE           NOT NULL,
  dy           NUMERIC(32, 8) NOT NULL,
  ev           NUMERIC(32, 8) NOT NULL,
  evebitda     NUMERIC(32, 8) NOT NULL,
  evps         NUMERIC(32, 8) NOT NULL,
  lydy         NUMERIC(32, 8) NOT NULL,
  negotiablemv NUMERIC(32, 8) NOT NULL,
  pb           NUMERIC(32, 8) NOT NULL,
  pclfy        NUMERIC(32, 8) NOT NULL,
  pcttm        NUMERIC(32, 8) NOT NULL,
  pelfy        NUMERIC(32, 8) NOT NULL,
  pelfynpaaei  NUMERIC(32, 8) NOT NULL,
  pemrq        NUMERIC(32, 8) NOT NULL,
  pemrqnpaaei  NUMERIC(32, 8) NOT NULL,
  pettm        NUMERIC(32, 8) NOT NULL,
  pettmnpaaei  NUMERIC(32, 8) NOT NULL,
  pslfy        NUMERIC(32, 8) NOT NULL,
  psmrq        NUMERIC(32, 8) NOT NULL,
  psttm        NUMERIC(32, 8) NOT NULL,
  tclose       NUMERIC(32, 8) NOT NULL,
  totmktcap    NUMERIC(32, 8) NOT NULL,
  turnrate     NUMERIC(32, 8) NOT NULL,
  total_share  NUMERIC(32, 8) NOT NULL,
  flow_share   NUMERIC(32, 8) NOT NULL,
  PRIMARY KEY (symbol, "end")
)