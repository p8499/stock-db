-- 对越大越好的KPI，本股在本行业的排名
CREATE OR REPLACE FUNCTION rpd(symbol CHAR, date DATE, kpi CHAR)
  RETURNS NUMERIC AS $$
# 本行业kpi列表
sql = 'SELECT symbol, %s(symbol, $2) AS kpi FROM industry_constituents t0 WHERE EXISTS (SELECT 1 FROM industry_constituents t1 WHERE t1.code = t0.code AND t1.symbol = $1) ORDER BY kpi DESC' % kpi
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
if len(rows) == 0:
    plpy.info('Not categorized to any industry, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
    return None
# 去null值
for row in rows:
    if row['kpi'] is None:
        rows.remove(row)
# 行号
index = len(rows) - 1
for i, row in enumerate(rows):
    if row['symbol'] == symbol:
        index = i
        break
# 计算
return (index + 1) / len(rows)
$$
LANGUAGE plpython3u;