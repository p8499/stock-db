--rate的n日内排名
CREATE OR REPLACE FUNCTION ratelodn(symbol CHAR, date DATE, n INT)
  RETURNS INT AS $$
# 查询rate数组
sql_rate = 'SELECT rate FROM (SELECT eob, rate(symbol, eob) AS rate FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT $3) s0 ORDER BY eob ASC'
plan_rate = plpy.prepare(sql_rate, ['CHAR', 'DATE', 'INT'])
rows_rate = list(plpy.execute(plan_rate, [symbol, date, n]))
# rate数据不足
if len(rows_rate) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s, $3 = %d' % (sql_rate, symbol, date, n))
    return None
# 不能有None行
if len(list(filter(lambda x: x['rate'] is None, rows_rate))) > 0:
    plpy.info('None value exists in rate array, sql = %s, $1 = %s, $2 = %s, $3 = %d' % (sql_rate, symbol, date, n))
    return None
# a数组
a = list(map(lambda x: x['rate'], rows_rate))
# lod(a, n)
sql_lod = 'SELECT lod($1, $2)'
plan_lod = plpy.prepare(sql_lod, ['NUMERIC[]', 'INT'])
rows_lod = list(plpy.execute(plan_lod, [a, n]))
return rows_lod[0]['lod']
$$
LANGUAGE plpython3u;