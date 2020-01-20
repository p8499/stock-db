-- 连续几年归属母公司利润增长
CREATE OR REPLACE FUNCTION grow(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
from datetime import datetime
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'grow', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'grow']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 预测的归属母公司利润列表（按日期倒序）
parenetp = []
# 查询最后的年报
sql_year = 'SELECT "end" FROM income_statement WHERE symbol = $1 AND pub <= $2 AND EXTRACT(MONTH FROM "end") = 12 ORDER BY "end" DESC LIMIT 1'
plan_year = plpy.prepare(sql_year, ['CHAR', 'DATE'])
rows_year = list(plpy.execute(plan_year, [symbol, date]))
if len(rows_year) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_year, symbol, date))
    return cachew(0)
year_date = rows_year[0]['end']
# 列表中依次推入最后年报后的非年报中的归属母公司利润
sql_latest_pl = 'SELECT "end", plforecast(symbol, pub, $4) AS plforecast FROM income_statement WHERE symbol = $1 AND pub <= $2 AND "end" > $3 ORDER BY "end" DESC'
plan_latest_pl = plpy.prepare(sql_latest_pl, ['CHAR', 'DATE', 'DATE', 'CHAR'])
rows_latest_pl = list(plpy.execute(plan_latest_pl, [symbol, date, year_date, 'parenetp']))
for row in rows_latest_pl:
    parenetp.append(float(row['plforecast']) if row['plforecast'] is not None else 0)
# 列表中依次推入之前所有年报（含最后年报）中的终归属母公司利润
sql_previous_pl = 'SELECT "end", parenetp FROM income_statement WHERE symbol = $1 AND pub <= $2 AND "end" <= $3 AND EXTRACT(MONTH FROM "end") = 12 ORDER BY "end" DESC'
plan_previous_pl = plpy.prepare(sql_previous_pl, ['CHAR', 'DATE', 'DATE'])
rows_previous_pl = list(plpy.execute(plan_previous_pl, [symbol, date, year_date]))
for row in rows_previous_pl:
    parenetp.append(float(row['parenetp']) if row['parenetp'] is not None else 0)
# 计算
# plpy.info(parenetp)
if len(parenetp) < 2:
    return cachew(0)
points = 0
# plpy.info(parenetp[:-1])
for i, row in enumerate(parenetp[:-1]):
    # plpy.info('row: %f' % row)
    this = row
    that = parenetp[i + 1]
    if 0 < that < this and this / that < 2:
        points += 1 / (2 ** i) * (this / that)
        # plpy.info('this: %f, that: %f, ADD: %f' % (this, that, 1 / (2 ** i) * (this / that)))
    elif 0 < that < this:
        points += 1 / (2 ** i) * 2
        # plpy.info('this: %f, that: %f, ADD: %f' % (this, that, 1 / (2 ** i) * 2))
return cachew(points)
$$
LANGUAGE plpython3u;