--ca的n日均值
CREATE OR REPLACE FUNCTION can(symbol CHAR, date DATE, n INT)
  RETURNS NUMERIC AS $$
# 查找n行ca
sql = 'SELECT ca(symbol, eob) AS ca FROM history_1d WHERE symbol = $1 and eob <= $2 ORDER BY eob DESC LIMIT $3'
plan = plpy.prepare(sql, ['CHAR', 'DATE', 'INT'])
rows = list(plpy.execute(plan, [symbol, date, n]))
# ca数据不足
if len(rows) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
    return None
# 不能有None行
if len(list(filter(lambda x: x['ca'] is None, rows))) > 0:
    plpy.info('None value exists in ca array, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
    return None
# 计算
return sum(map(lambda x: x['ca'], rows)) / len(rows)
$$
LANGUAGE plpython3u;