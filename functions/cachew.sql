-- 写cache
CREATE OR REPLACE FUNCTION cachew(symbol CHAR, date DATE, kpi VARCHAR, val NUMERIC)
  RETURNS VOID AS $$
# select
sql_select = 'SELECT COUNT(*) AS count FROM cache WHERE symbol = $1 AND date = $2 AND kpi = $3'
plan_select = plpy.prepare(sql_select, ['CHAR', 'DATE', 'VARCHAR'])
rows_select = list(plpy.execute(plan_select, [symbol, date, kpi]))
# update or insert
if rows_select[0]['count'] > 0:
    sql_update = 'UPDATE cache SET val = $4 WHERE symbol = $1 AND date = $2 AND kpi = $3'
    plan_update = plpy.prepare(sql_update, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_update, [symbol, date, kpi, val])
else:
    sql_insert = 'INSERT INTO cache (symbol, date, kpi, val) VALUES ($1, $2, $3, $4)'
    plan_insert = plpy.prepare(sql_insert, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_insert, [symbol, date, kpi, val])
$$
LANGUAGE plpython3u;