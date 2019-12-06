--rate的n日均值
CREATE OR REPLACE FUNCTION raten(symbol CHAR, date DATE, n INT)
  RETURNS NUMERIC AS $$
# 查找n行rate
sql = 'SELECT rate(symbol, eob) AS rate FROM history_1d WHERE symbol = $1 and eob <= $2 ORDER BY eob DESC LIMIT $3'
plan = plpy.prepare(sql, ['CHAR', 'DATE', 'INT'])
rows = list(plpy.execute(plan, [symbol, date, n]))
# rate数据不足
if len(rows) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s, $3 = %d' % (sql, symbol, date, n))
    return None
# 不能有None行
if len(list(filter(lambda x: x['rate'] is None, rows))) > 0:
    plpy.info('None value exists in rate array, sql = %s, $1 = %s, $2 = %s, $3 = %d' % (sql, symbol, date, n))
    return None
# 计算
return sum(map(lambda x: x['rate'], rows)) / len(rows)
$$
LANGUAGE plpython3u;