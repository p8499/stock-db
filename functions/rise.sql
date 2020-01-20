-- 单日涨幅
CREATE OR REPLACE FUNCTION rise(symbol CHAR, date0 DATE, date1 DATE)
  RETURNS NUMERIC AS $$
# 查询date0当天收盘价
sql0 = 'SELECT ca(symbol, eob) AS ca FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan0 = plpy.prepare(sql0, ['CHAR', 'DATE'])
rows0 = list(plpy.execute(plan0, [symbol, date0]))
# 至少需要1条数据，不可为None，不可为0
if len(rows0) < 1 or rows0[0]['ca'] is None or rows0[0]['ca'] == 0:
    plpy.info('At least 1 non-null and non-zero records required to calculate RISE, sql = %s, $1 = %s, $2 = %s' % (sql0, symbol, date0))
    return None
# 查询date0当天收盘价
sql1 = 'SELECT ca(symbol, eob) AS ca FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
plan1 = plpy.prepare(sql1, ['CHAR', 'DATE'])
rows1 = list(plpy.execute(plan1, [symbol, date1]))
# 至少需要1条数据，不可为None，不可为0
if len(rows1) < 1 or rows1[0]['ca'] is None or rows1[0]['ca'] == 0:
    plpy.info('At least 1 non-null and non-zero records required to calculate RISE, sql = %s, $1 = %s, $2 = %s' % (sql1, symbol, date1))
    return None
# 计算
return (rows1[0]['ca'] - rows0[0]['ca']) / rows0[0]['ca']
$$
LANGUAGE plpython3u