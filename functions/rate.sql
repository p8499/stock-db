-- 当前股价是平均成本的几倍
CREATE OR REPLACE FUNCTION rate(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'rate', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'rate']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 查找mcst
sql_mcst = 'SELECT mcst($1, $2)'
plan_mcst = plpy.prepare(sql_mcst, ['CHAR', 'DATE'])
rows_mcst = list(plpy.execute(plan_mcst, [symbol, date]))
mcst = rows_mcst[0]['mcst']
if mcst is None or mcst == 0:
    plpy.info('Cannot calculate RATE due to MCST is none or zero, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 查找ca
sql_ca = 'SELECT ca($1, $2)'
plan_ca = plpy.prepare(sql_ca, ['CHAR', 'DATE'])
rows_ca = list(plpy.execute(plan_ca, [symbol, date]))
ca = rows_ca[0]['ca']
if ca is None:
    plpy.info('Cannot calculate RATE due to CA is none, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 计算
return cachew(ca / mcst)
$$
LANGUAGE plpython3u;