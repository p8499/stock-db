-- 后复权，即×复权因子的平均市场成本
CREATE OR REPLACE FUNCTION mcst(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'mcst', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'mcst']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 构建{eob, amount, volume}数组
sql = 'SELECT eob, amount, volume FROM (SELECT * FROM history_1d WHERE symbol = $1 AND eob <= $2 ORDER BY eob DESC LIMIT 500) s0 ORDER BY eob ASC'
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
# 补全{eob, amount, volume, adj_factor, flow_share}数组
for row in rows:
    # 追加adj_factor列
    sql_adj_factor = 'SELECT adj_factor FROM history_instruments WHERE symbol = $1 AND trade_date <= $2 ORDER BY trade_date DESC LIMIT 1'
    plan_adj_factor = plpy.prepare(sql_adj_factor, ['CHAR', 'DATE'])
    rows_adj_factor = list(plpy.execute(plan_adj_factor, [symbol, row['eob']]))
    if len(rows_adj_factor) == 1:
        row['adj_factor'] = rows_adj_factor[0]['adj_factor']
    else:
        plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_adj_factor, symbol, row['eob']))
        return cachew(None)
    # 追加flow_share列
    sql_flow_share = 'SELECT flow_share FROM trading_derivative_indicator WHERE symbol = $1 AND "end" <= $2 ORDER BY "end" DESC LIMIT 1'
    plan_flow_share = plpy.prepare(sql_flow_share, ['CHAR', 'DATE'])
    rows_flow_share = list(plpy.execute(plan_flow_share, [symbol, row['eob']]))
    if len(rows_flow_share) == 1:
        row['flow_share'] = rows_flow_share[0]['flow_share']
    else:
        plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_flow_share, symbol, row['eob']))
        return cachew(None)
# 去除无法计算的数据
for row in reversed(rows):
    if row['volume'] == 0 or row['flow_share'] == 0:
        rows.remove(row)
if len(rows) == 0:
    plpy.info('No data can be used to calculate MCST, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# x数组
x = list(map(lambda x: x['amount'] / x['volume'] * x['adj_factor'], rows))
# a数组
a = list(map(lambda x: x['volume'] / x['flow_share'], rows))
# dma(x, a)
sql_dma = 'SELECT dma($1, $2)'
plan_dma = plpy.prepare(sql_dma, ['NUMERIC[]', 'NUMERIC[]'])
rows_dma = list(plpy.execute(plan_dma, [x, a]))
# 计算
return cachew(rows_dma[0]['dma'])
$$
LANGUAGE plpython3u;

