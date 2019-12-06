-- 策略函数，即为真时，代表亮起卖出信号
CREATE OR REPLACE FUNCTION s10(symbol CHAR, date DATE)
  RETURNS BOOLEAN AS $$
# 获得最近的3组can
sql = 'SELECT ca5(symbol, eob) AS ca5, ca10(symbol, eob) AS ca10, ca20(symbol, eob) AS ca20, ca60(symbol, eob) AS ca60, ca120(symbol, eob) AS ca120, ca250(symbol, eob) AS ca250 FROM history_1d WHERE symbol = $1 and eob <= $2 ORDER BY eob DESC LIMIT 3'
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
# 数据不足
if len(rows) < 3:
    plpy.info('Insufficient data for calculate S10, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
    return None
# 不允许None行
if rows[0]['ca5'] is None or rows[0]['ca10'] is None or rows[0]['ca20'] is None or rows[0]['ca60'] is None or rows[0]['ca120'] is None or rows[0]['ca250'] is None \
    or rows[1]['ca5'] is None or rows[1]['ca5'] == 0 or rows[1]['ca10'] is None or rows[1]['ca10'] == 0\
    or rows[2]['ca5'] is None or rows[2]['ca5'] == 0 or rows[2]['ca10'] is None or rows[2]['ca10'] == 0:
    plpy.info('None or zero value exists, sql = %s, $1 = %s, $2 = %s' % (sql_can, symbol, date))
    return None
# 挡位
gear = rows[0]['ca250'] <= rows[0]['ca120'] and rows[0]['ca120'] <= rows[0]['ca60'] and rows[0]['ca60'] <= rows[0]['ca20'] and rows[0]['ca20'] <= rows[0]['ca10']
if not gear:
    return False
return (rows[0]['ca5'] - rows[1]['ca5']) / rows[1]['ca5'] < (rows[1]['ca5'] - rows[2]['ca5']) / rows[2]['ca5'] \
    and (rows[0]['ca10'] - rows[1]['ca10']) / rows[1]['ca10'] < (rows[1]['ca10'] - rows[2]['ca10']) / rows[2]['ca10']
$$
LANGUAGE plpython3u;