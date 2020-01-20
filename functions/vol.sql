-- 交易量占流通比
CREATE OR REPLACE FUNCTION vfr(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
# 查volume
sql_volume = 'SELECT volume FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan_volume = plpy.prepare(sql_volume, ['CHAR', 'DATE'])
rows_volume = list(plpy.execute(plan_volume, [symbol, date]))
if len(rows_volume) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_volume, symbol, date))
    return None
volume = rows_volume[0]['volume']
# 查flow_share
sql_share = 'SELECT flow_share FROM trading_derivative_indicator WHERE symbol = $1 AND "end" <= $2 ORDER BY "end" DESC LIMIT 1'
plan_share = plpy.prepare(sql_share, ['CHAR', 'DATE'])
rows_share = list(plpy.execute(plan_share, [symbol, date]))
if len(rows_share) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_share, symbol, date))
flow_share = rows_share[0]['flow_share']
if flow_share <= 0:
    plpy.info('Unable to calculate VOL due to zero or negative share, sql = %s, $1 = %s, $2 = %s' % (sql_share, symbol, date))
    return None
return volume / flow_share
$$
LANGUAGE plpython3u;