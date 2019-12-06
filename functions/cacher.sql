-- 读cache
CREATE OR REPLACE FUNCTION cacher(symbol CHAR, date DATE, kpi VARCHAR, OUT yn BOOLEAN, OUT val NUMERIC) AS $$
sql = 'SELECT val FROM cache WHERE symbol = $1 AND date = $2 AND kpi = $3'
plan = plpy.prepare(sql, ['CHAR', 'DATE', 'VARCHAR'])
rows = list(plpy.execute(plan, [symbol, date, kpi]))
if len(rows) == 1:
    return (True, rows[0]['val'])
else:
    return (False, None)
$$
LANGUAGE plpython3u;