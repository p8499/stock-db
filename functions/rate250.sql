--rate的250日均值
CREATE OR REPLACE FUNCTION rate250(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'rate250', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'rate250']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 查找
sql = 'SELECT raten($1, $2, $3)'
plan = plpy.prepare(sql, ['CHAR', 'DATE', 'INT'])
rows = list(plpy.execute(plan, [symbol, date, 250]))
# 计算
return cachew(rows[0]['raten'])
$$
LANGUAGE plpython3u;