-- power势力值
CREATE OR REPLACE FUNCTION pow(date DATE, bpol CHAR)
  RETURNS INT AS $$
# count
sql = 'SELECT count(1) AS count FROM power WHERE bob <= $1 AND (eob > $1 OR eob IS NULL) AND bpol = $2'
plan = plpy.prepare(sql, ['DATE', 'CHAR'])
rows = list(plpy.execute(plan, [date, bpol]))
count = rows[0]['count']
# 返回
return count
$$
LANGUAGE plpython3u;