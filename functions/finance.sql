-- 策略函数，即为真时，代表通过价值过滤要求
CREATE OR REPLACE FUNCTION fin2(symbol CHAR, date DATE)
  RETURNS BOOLEAN AS $$
sql = "SELECT 1" \
    "WHERE rpa($1, $2, 'dar') <= 0.9" \
    "AND rpd($1, $2, 'er') <= 0.9" \
    "AND rpa($1, $2, 'wcr') <= 0.9" \
    "AND rpa($1, $2, 'qr') <= 0.9" \
    "AND rpa($1, $2, 'cr') <= 0.9" \
    "AND rpa($1, $2, 'ltlr') <= 0.9" \
    "AND rpd($1, $2, 'roe') <= 0.7" \
    "AND rpa($1, $2, 'em') <= 0.8"
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
return len(rows) > 0
$$
LANGUAGE plpython3u;