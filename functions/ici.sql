-- 行业综合指数
CREATE OR REPLACE FUNCTION ici(code CHAR, date DATE)
  RETURNS NUMERIC AS $$
# 构建{symbol}数组
sql = 'SELECT symbol FROM industry_constituents WHERE code = $1'
plan = plpy.prepare(sql, ['CHAR'])
rows = list(plpy.execute(plan, [code]))
# 补全{symbol, close, adj_factor, total_share}数组
for row in reversed(rows):
    # 追加close列
    sql_close = 'SELECT close FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 1'
    plan_close = plpy.prepare(sql_close, ['CHAR', 'DATE'])
    rows_close = list(plpy.execute(plan_close, [row['symbol'], date]))
    if len(rows_close) == 1:
        row['close'] = rows_close[0]['close']
    else:
        plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_close, row['symbol'], date))
        rows.remove(row)
        continue
    # 追加adj_factor列
    sql_adj_factor = 'SELECT adj_factor FROM history_instruments WHERE symbol = $1 AND trade_date <= $2 ORDER BY trade_date DESC LIMIT 1'
    plan_adj_factor = plpy.prepare(sql_adj_factor, ['CHAR', 'DATE'])
    rows_adj_factor = list(plpy.execute(plan_adj_factor, [row['symbol'], date]))
    if len(rows_adj_factor) == 1:
        row['adj_factor'] = rows_adj_factor[0]['adj_factor']
    else:
        plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_adj_factor, row['symbol'], date))
        rows.remove(row)
        continue
    # 追加total_share列
    sql_flow_share = 'SELECT total_share FROM trading_derivative_indicator WHERE symbol = $1 AND "end" <= $2 ORDER BY "end" DESC LIMIT 1'
    plan_flow_share = plpy.prepare(sql_flow_share, ['CHAR', 'DATE'])
    rows_flow_share = list(plpy.execute(plan_flow_share, [row['symbol'], date]))
    if len(rows_flow_share) == 1:
        row['total_share'] = rows_flow_share[0]['total_share']
    else:
        plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_flow_share, row['symbol'], date))
        rows.remove(row)
        continue
# 计算
sum = 0
for row in rows:
    sum += row['close'] * row['adj_factor'] * row['total_share']
return sum
$$
LANGUAGE plpython3u;
