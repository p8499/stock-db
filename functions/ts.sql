-- 总股数
CREATE OR REPLACE FUNCTION ts(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'ts', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'ts']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 市盈率
sql_pelfy = 'SELECT pelfy FROM trading_derivative_indicator WHERE symbol = $1 AND "end" <= $2 ORDER BY "end" DESC LIMIT 1'
plan_pelfy = plpy.prepare(sql_pelfy, ['CHAR', 'DATE'])
rows_pelfy = list(plpy.execute(plan_pelfy, [symbol, date]))
if len(rows_pelfy) == 0:
    plpy.info('Insufficient data to calculate TS, sql = %s, $1 = %s, $2 = %s' % (sql_pelfy, symbol, date))
    return cachew(None)
pelfy = rows_pelfy[0]['pelfy']
# 归属母公司所有者的净利润。找到离当时最近的一张年报，且17年年报最多用到19年4月(19年4月必须读到18年年报)
sql_parenetp = 'SELECT parenetp FROM income_statement WHERE symbol = $1 AND pub <= $2 AND (' \
    'EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 16 AND EXTRACT(MONTH FROM "end") = 12' \
    ') ORDER BY "end" DESC LIMIT 1'
plan_parenetp = plpy.prepare(sql_parenetp, ['CHAR', 'DATE'])
rows_parenetp = list(plpy.execute(plan_parenetp, [symbol, date]))
if len(rows_parenetp) == 0:
    plpy.info('Insufficient data to calculate TS, sql = %s, $1 = %s, $2 = %s' % (sql_parenetp, symbol, date))
    return cachew(None)
parenetp = rows_parenetp[0]['parenetp']
# 收盘价
sql_close = 'SELECT close FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan_close = plpy.prepare(sql_close, ['CHAR', 'DATE'])
rows_close = list(plpy.execute(plan_close, [symbol, date]))
if len(rows_close) == 0:
    plpy.info('Insufficient data for calculate TS, sql = %s, $1 = %s, $2 = %s' % (sql_close, symbol, date))
    return cachew(None)
if rows_close[0]['close'] == 0:
    plpy.info('Cannot get a proper CLOSE to calculate TS, sql = %s, $1 = %s, $2 = %s' % (sql_close, symbol, date))
    return cachew(None)
close = rows_close[0]['close']
# 计算
return cachew(pelfy * parenetp / close)
$$
LANGUAGE plpython3u;