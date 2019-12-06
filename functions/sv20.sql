-- 策略函数，即为真时，代表亮起卖出信号
CREATE OR REPLACE FUNCTION sv20(symbol CHAR, date DATE)
  RETURNS BOOLEAN AS $$
# 获得最近的一条can
sql_can = 'SELECT ca10(symbol, eob) AS ca10, ca20(symbol, eob) AS ca20, ca60(symbol, eob) AS ca60, ca120(symbol, eob) AS ca120, ca250(symbol, eob) AS ca250 FROM history_1d WHERE symbol = $1 and eob <= $2 ORDER BY eob DESC LIMIT 1'
plan_can = plpy.prepare(sql_can, ['CHAR', 'DATE'])
rows_can = list(plpy.execute(plan_can, [symbol, date]))
# 数据不足
if len(rows_can) < 1:
    plpy.info('Insufficient data for calculate SV20, sql = %s, $1 = %s, $2 = %s' % (sql_can, symbol, date))
    return None
# 不允许None行
if rows_can[0]['ca10'] is None or rows_can[0]['ca20'] is None or rows_can[0]['ca60'] is None or rows_can[0]['ca120'] is None or rows_can[0]['ca250'] is None:
    plpy.info('Cannot calculate SV20 due to some data are None, sql = %s, $1 = %s, $2 = %s' % (sql_can, symbol, date))
    return None
# 挡位
gear = rows_can[0]['ca250'] < rows_can[0]['ca120'] < rows_can[0]['ca60'] < rows_can[0]['ca20'] and rows_can[0]['ca10'] <= rows_can[0]['ca20']
if not gear:
    return False
# 构建{eob, close, high, low, volume}数组
sql = 'SELECT eob, close, high, low, volume FROM (SELECT eob, close, high, low, volume FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 4 + 1) s1 ORDER BY eob ASC'
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
if len(rows) < 4 + 1:
    plpy.info('%s rows are too few to proceed, sql = %s, $1 = %s, $2 = %s' % (rows, sql, symbol, date))
    return None
for row in rows:
    if row['close'] == 0 or row['high'] == 0 or row['low'] == 0:
        plpy.info('Close or High or Low is zero, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
        return None
    # 追加adj_factor列
    sql_adj_factor = 'SELECT adj_factor FROM history_instruments WHERE symbol = $1 AND trade_date <= $2 ORDER BY trade_date DESC LIMIT 1'
    plan_adj_factor = plpy.prepare(sql_adj_factor, ['CHAR', 'DATE'])
    rows_adj_factor = list(plpy.execute(plan_adj_factor, [symbol, row['eob']]))
    if len(rows_adj_factor) == 1 and rows_adj_factor[0]['adj_factor'] != 0:
        row['adj_factor'] = rows_adj_factor[0]['adj_factor']
    else:
        plpy.info('Insufficient or zero data, sql = %s, $1 = %s, $2 = %s' % (sql_adj_factor, symbol, row['eob']))
        return None
    # 追加flow_share列
    sql_flow_share = 'SELECT flow_share FROM trading_derivative_indicator WHERE symbol = $1 AND "end" <= $2 ORDER BY "end" DESC LIMIT 1'
    plan_flow_share = plpy.prepare(sql_flow_share, ['CHAR', 'DATE'])
    rows_flow_share = list(plpy.execute(plan_flow_share, [symbol, row['eob']]))
    if len(rows_flow_share) == 1 and rows_flow_share[0]['flow_share'] != 0:
        row['flow_share'] = rows_flow_share[0]['flow_share']
    else:
        plpy.info('Insufficient or zero data, sql = %s, $1 = %s, $2 = %s' % (sql_flow_share, symbol, row['eob']))
        return None
    # 追加closer1列
    sql_closer1 = 'SELECT close FROM (SELECT eob, close FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 2) s1 ORDER BY eob ASC LIMIT 1'
    plan_closer1 = plpy.prepare(sql_closer1, ['CHAR', 'DATE'])
    rows_closer1 = list(plpy.execute(plan_closer1, [symbol, row['eob']]))
    if len(rows_closer1) == 1 and rows_closer1[0]['close'] != 0:
        row['closer1'] = rows_closer1[0]['close']
    else:
        plpy.info('Insufficient or zero data, sql = %s, $1 = %s, $2 = %s' % (sql_closer1, symbol, row['eob']))
        return None
    # 追加vibrate列（波动幅度）
    turnover = row['volume'] / row['flow_share']
    h = max(row['high'] * row['adj_factor'], row['closer1'] * row['adj_factor'])
    l = min(row['low'] * row['adj_factor'], row['closer1'] * row['adj_factor'])
    percent = (h - l) * 100 / l
    vibrate = turnover * percent
    row['vibrate'] = vibrate
# 计算toprange
sql_toprange = 'SELECT toprange($1)'
plan_toprange = plpy.prepare(sql_toprange, ['NUMERIC[]'])
rows_toprange = list(plpy.execute(plan_toprange, [list(map(lambda x: x['vibrate'], rows))]))
toprange = rows_toprange[0]['toprange']
# 计算
return toprange>= 4 and rows[-1]['close'] > rows[-2]['close']
$$
LANGUAGE plpython3u;

-- select symbol, eob, sv20(symbol, eob) from history_1d where symbol = 'SZSE.300015' and eob between '2018-01-01' and '2018-03-31';
