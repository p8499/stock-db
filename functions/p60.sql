--策略函数，即为真时，代表亮起买入信号
CREATE OR REPLACE FUNCTION p60(symbol CHAR, date DATE)
  RETURNS BOOLEAN AS $$
# 最低条数
sql_count = 'SELECT COUNT(*) AS count FROM history_1d WHERE symbol = $1 AND eob <= $2'
plan_count = plpy.prepare(sql_count, ['CHAR', 'DATE'])
rows_count = list(plpy.execute(plan_count, [symbol, date]))
if rows_count[0]['count'] < 250:
    plpy.info('%s rows are too few to proceed, sql = %s, $1 = %s, $2 = %s' % (rows_count[0]['count'], sql_count, symbol, date))
    return None
# 获得最近的两组can
sql_can = 'SELECT ca60(symbol, eob) AS ca60, ca120(symbol, eob) AS ca120, ca250(symbol, eob) AS ca250 FROM history_1d WHERE symbol = $1 and eob <= $2 ORDER BY eob DESC LIMIT 2'
plan_can = plpy.prepare(sql_can, ['CHAR', 'DATE'])
rows_can = list(plpy.execute(plan_can, [symbol, date]))
# 数据不足
if len(rows_can) < 2:
    plpy.info('Insufficient data for calculate P60, sql = %s, $1 = %s, $2 = %s' % (sql_can, symbol, date))
    return None
# 不允许None行
for row in rows_can:
    if row['ca60'] is None or row['ca120'] is None or row['ca250'] is None:
        plpy.info('Cannot calculate P60 due to some data are None, sql = %s, $1 = %s, $2 = %s' % (sql_can, symbol, date))
        return None
# 挡位
gear = rows_can[0]['ca60'] >= rows_can[1]['ca60'] and rows_can[0]['ca120'] >= rows_can[1]['ca120'] and rows_can[0]['ca250'] >= rows_can[1]['ca250']
if not gear:
    return False
# 3条ratelod60数据
sql_ratelod = 'SELECT ratelod60(symbol, eob) ratelod60 FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 3'
plan_ratelod = plpy.prepare(sql_ratelod, ['CHAR', 'DATE'])
rows_ratelod = list(plpy.execute(plan_ratelod, [symbol, date]))
# 数据不足
if len(rows_ratelod) < 3:
    plpy.info('Insufficient data for calculate P60, sql = %s, $1 = %s, $2 = %s' % (sql_ratelod, symbol, date))
    return None
# 不允许None行
for row in rows_ratelod:
    if row['ratelod60'] is None:
        plpy.info('Cannot calculate P60 due to some data are None, sql = %s, $1 = %s, $2 = %s' % (sql_ratelod, symbol, date))
        return None
# 计算
return rows_ratelod[2]['ratelod60'] >= rows_ratelod[1]['ratelod60'] and rows_ratelod[0]['ratelod60'] > rows_ratelod[1]['ratelod60'] and rows_ratelod[1]['ratelod60'] == 1
$$
LANGUAGE plpython3u;