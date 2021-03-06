DROP TABLE IF EXISTS cashflow_statement;
CREATE TABLE cashflow_statement (
  symbol             CHAR(11)       NOT NULL,
  pub                DATE           NOT NULL,
  "end"              DATE           NOT NULL,
  cashnetr           NUMERIC(32, 8) NOT NULL,
  mananetr           NUMERIC(32, 8) NOT NULL,
  bizcashinfl        NUMERIC(32, 8) NOT NULL,
  laborgetcash       NUMERIC(32, 8) NOT NULL,
  deponetr           NUMERIC(32, 8) NOT NULL,
  bankloannetincr    NUMERIC(32, 8) NOT NULL,
  fininstnetr        NUMERIC(32, 8) NOT NULL,
  inspremcash        NUMERIC(32, 8) NOT NULL,
  insnetc            NUMERIC(32, 8) NOT NULL,
  savinetr           NUMERIC(32, 8) NOT NULL,
  disptradnetincr    NUMERIC(32, 8) NOT NULL,
  charintecash       NUMERIC(32, 8) NOT NULL,
  fdsborrnetr        NUMERIC(32, 8) NOT NULL,
  repnetincr         NUMERIC(32, 8) NOT NULL,
  taxrefd            NUMERIC(32, 8) NOT NULL,
  receotherbizcash   NUMERIC(32, 8) NOT NULL,
  bizcashoutf        NUMERIC(32, 8) NOT NULL,
  labopayc           NUMERIC(32, 8) NOT NULL,
  loansnetr          NUMERIC(32, 8) NOT NULL,
  tradepaymnetr      NUMERIC(32, 8) NOT NULL,
  paycompgold        NUMERIC(32, 8) NOT NULL,
  payintecash        NUMERIC(32, 8) NOT NULL,
  paydivicash        NUMERIC(32, 8) NOT NULL,
  payworkcash        NUMERIC(32, 8) NOT NULL,
  paytax             NUMERIC(32, 8) NOT NULL,
  payacticash        NUMERIC(32, 8) NOT NULL,
  invnetcashflow     NUMERIC(32, 8) NOT NULL,
  invcashinfl        NUMERIC(32, 8) NOT NULL,
  withinvgetcash     NUMERIC(32, 8) NOT NULL,
  inveretugetcash    NUMERIC(32, 8) NOT NULL,
  fixedassetnetc     NUMERIC(32, 8) NOT NULL,
  subsnetc           NUMERIC(32, 8) NOT NULL,
  receinvcash        NUMERIC(32, 8) NOT NULL,
  reducashpled       NUMERIC(32, 8) NOT NULL,
  invcashoutf        NUMERIC(32, 8) NOT NULL,
  acquassetcash      NUMERIC(32, 8) NOT NULL,
  invpayc            NUMERIC(32, 8) NOT NULL,
  loannetr           NUMERIC(32, 8) NOT NULL,
  subspaynetcash     NUMERIC(32, 8) NOT NULL,
  payinvecash        NUMERIC(32, 8) NOT NULL,
  incrcashpled       NUMERIC(32, 8) NOT NULL,
  finnetcflow        NUMERIC(32, 8) NOT NULL,
  fincashinfl        NUMERIC(32, 8) NOT NULL,
  invrececash        NUMERIC(32, 8) NOT NULL,
  subsrececash       NUMERIC(32, 8) NOT NULL,
  recefromloan       NUMERIC(32, 8) NOT NULL,
  issbdrececash      NUMERIC(32, 8) NOT NULL,
  recefincash        NUMERIC(32, 8) NOT NULL,
  fincashoutf        NUMERIC(32, 8) NOT NULL,
  debtpaycash        NUMERIC(32, 8) NOT NULL,
  diviprofpaycash    NUMERIC(32, 8) NOT NULL,
  subspaydivid       NUMERIC(32, 8) NOT NULL,
  finrelacash        NUMERIC(32, 8) NOT NULL,
  chgexchgchgs       NUMERIC(32, 8) NOT NULL,
  inicashbala        NUMERIC(32, 8) NOT NULL,
  finalcashbala      NUMERIC(32, 8) NOT NULL,
  netprofit          NUMERIC(32, 8) NOT NULL,
  minysharrigh       NUMERIC(32, 8) NOT NULL,
  unreinveloss       NUMERIC(32, 8) NOT NULL,
  asseimpa           NUMERIC(32, 8) NOT NULL,
  assedepr           NUMERIC(32, 8) NOT NULL,
  intaasseamor       NUMERIC(32, 8) NOT NULL,
  longdefeexpenamor  NUMERIC(32, 8) NOT NULL,
  prepexpedecr       NUMERIC(32, 8) NOT NULL,
  accrexpeincr       NUMERIC(32, 8) NOT NULL,
  dispfixedassetloss NUMERIC(32, 8) NOT NULL,
  fixedassescraloss  NUMERIC(32, 8) NOT NULL,
  valuechgloss       NUMERIC(32, 8) NOT NULL,
  defeincoincr       NUMERIC(32, 8) NOT NULL,
  estidebts          NUMERIC(32, 8) NOT NULL,
  finexpe            NUMERIC(32, 8) NOT NULL,
  inveloss           NUMERIC(32, 8) NOT NULL,
  defetaxassetdecr   NUMERIC(32, 8) NOT NULL,
  defetaxliabincr    NUMERIC(32, 8) NOT NULL,
  inveredu           NUMERIC(32, 8) NOT NULL,
  receredu           NUMERIC(32, 8) NOT NULL,
  payaincr           NUMERIC(32, 8) NOT NULL,
  unseparachg        NUMERIC(32, 8) NOT NULL,
  unfiparachg        NUMERIC(32, 8) NOT NULL,
  other              NUMERIC(32, 8) NOT NULL,
  biznetcflow        NUMERIC(32, 8) NOT NULL,
  debtintocapi       NUMERIC(32, 8) NOT NULL,
  expiconvbd         NUMERIC(32, 8) NOT NULL,
  finfixedasset      NUMERIC(32, 8) NOT NULL,
  cashfinalbala      NUMERIC(32, 8) NOT NULL,
  cashopenbala       NUMERIC(32, 8) NOT NULL,
  equfinalbala       NUMERIC(32, 8) NOT NULL,
  equopenbala        NUMERIC(32, 8) NOT NULL,
  cashneti           NUMERIC(32, 8) NOT NULL,
  realestadep        NUMERIC(32, 8) NOT NULL,
  PRIMARY KEY (symbol, "end")
)