-- 根据预测年报KPI反算市盈率
CREATE OR REPLACE FUNCTION pe(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'pe', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'pe']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 收盘价
sql_close = 'SELECT close FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan_close = plpy.prepare(sql_close, ['CHAR', 'DATE'])
rows_close = list(plpy.execute(plan_close, [symbol, date]))
if len(rows_close) == 0:
    plpy.info('Cannot calculate PE due to insufficient value of CLOSE, sql = %s, $1 = %s, $2 = %s' % (sql_close, symbol, date))
    return cachew(None)
close = rows_close[0]['close']
# 总股本
sql_ts = 'SELECT ts($1, $2)'
plan_ts = plpy.prepare(sql_ts, ['CHAR', 'DATE'])
rows_ts = list(plpy.execute(plan_ts, [symbol, date]))
ts = rows_ts[0]['ts']
if ts is None or ts == 0:
    plpy.info('Cannot calculate PE due to none or zero value of TS, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 母公司净利润
sql_parenetp = 'SELECT plforecast($1, $2, $3)'
plan_parenetp = plpy.prepare(sql_parenetp, ['CHAR', 'DATE', 'CHAR'])
rows_parenetp = list(plpy.execute(plan_parenetp, [symbol, date, 'parenetp']))
parenetp = rows_parenetp[0]['plforecast']
if parenetp is None or parenetp <= 0:
    plpy.info('Cannot calculate PE due to none or zero/negative value of PARENETP, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 计算
return cachew(close * ts / parenetp)
$$
LANGUAGE plpython3u;