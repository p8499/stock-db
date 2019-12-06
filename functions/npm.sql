-- 销售净利润率
CREATE OR REPLACE FUNCTION npm(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'npm', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'npm']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 净利润
sql_pl1 = 'SELECT plforecast($1, $2, $3)'
plan_pl1 = plpy.prepare(sql_pl1, ['CHAR', 'DATE', 'CHAR'])
rows_pl1 = list(plpy.execute(plan_pl1, [symbol, date, 'netprofit']))
netprofit = rows_pl1[0]['plforecast']
if netprofit is None:
    plpy.info('Cannot calculate NPM due to none value of NETPROFIT, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 营业总收入
sql_pl2 = 'SELECT plforecast($1, $2, $3)'
plan_pl2 = plpy.prepare(sql_pl2, ['CHAR', 'DATE', 'CHAR'])
rows_pl2 = list(plpy.execute(plan_pl2, [symbol, date, 'biztotinco']))
biztotinco = rows_pl2[0]['plforecast']
if biztotinco is None or biztotinco == 0:
    plpy.info('Cannot calculate ROE due to none or zero value of BIZTOTINCO, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 计算
return cachew(netprofit / biztotinco)
$$
LANGUAGE plpython3u;