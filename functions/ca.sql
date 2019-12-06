-- 后复权，即×复权因子的当前股价
CREATE OR REPLACE FUNCTION ca(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'ca', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'ca']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 查找close
sql_close = 'SELECT eob, close FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan_close = plpy.prepare(sql_close, ['CHAR', 'DATE'])
rows_close = list(plpy.execute(plan_close, [symbol, date]))
if len(rows_close) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_close, symbol, date))
    return cachew(None)
close = rows_close[0]['close']
eob = rows_close[0]['eob']
# 查找adj_factor
sql_adj = 'SELECT adj_factor FROM history_instruments WHERE symbol = $1 AND trade_date <= $2 ORDER BY trade_date DESC LIMIT 1'
plan_adj = plpy.prepare(sql_adj, ['CHAR', 'DATE'])
rows_adj = list(plpy.execute(plan_adj, [symbol, eob]))
if len(rows_adj) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_adj, symbol, date))
    return cachew(None)
adj_factor = rows_adj[0]['adj_factor']
# 计算
return cachew(close * adj_factor)
$$
LANGUAGE plpython3u;