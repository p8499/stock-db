-- 连续几年归属母公司利润增长
CREATE OR REPLACE FUNCTION grow(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
from datetime import datetime
# 查询最近一条利润表
sql_last_pl = 'SELECT "end", parenetp FROM income_statement WHERE symbol = $1 AND pub <= $2 ORDER BY "end" DESC LIMIT 1'
plan_last_pl = plpy.prepare(sql_last_pl, ['CHAR', 'DATE'])
rows_last_pl = list(plpy.execute(plan_last_pl, [symbol, date]))
if len(rows_last_pl) == 0:
    plpy.info('Insufficient data, sql = %s, $1 = %s, $2 = %s' % (sql_last_pl, symbol, date))
    return 0
# 计算最近的归属母公司利润
parenetp = 0
# 依照以下两种情况，计算最近的归属母公司利润parenetp
last_parenetp = rows_last_pl[0]['parenetp']
last_end = rows_last_pl[0]['end']
if int(datetime.strptime(last_end, '%Y-%m-%d').strftime('%m')) < 12:
    # 如果利润表最后一条不是年报，则预测它的当年归属母公司利润
    sql_parenetp_0 = 'SELECT plforecast($1, $2, $3) as plforecast'
    plan_parenetp_0 = plpy.prepare(sql_parenetp_0, ['CHAR', 'DATE', 'CHAR'])
    rows_parenetp_0 = list(plpy.execute(plan_parenetp_0, [symbol, date, 'parenetp']))
    if rows_parenetp_0[0]['plforecast'] is None:
        plpy.info('Result is null, unable to calculate GROW, sql = %s, $1 = %s, $2 = %s, $2 = %s' % (sql_parenetp_0, symbol, date, 'parenetp'))
        return 0
    parenetp = rows_parenetp_0[0]['plforecast']
else:
    # 如果利润表最后一条是年报，则直接取它的归属母公司利润
    parenetp = last_parenetp
# 取出之前的所有年终归属母公司利润
sql_parenetp_n = 'SELECT "end", parenetp FROM income_statement WHERE symbol = $1 AND pub <= $2 AND "end" < $3 AND EXTRACT(MONTH FROM "end") = 12 ORDER BY "end" DESC'
plan_parenetp_n = plpy.prepare(sql_parenetp_n, ['CHAR', 'DATE', 'DATE'])
rows_parenetp_n = list(plpy.execute(plan_parenetp_n, [symbol, date, last_end]))
# 计算
points = 0
for i, row in enumerate(rows_parenetp_n):
    if row['parenetp'] < parenetp:
        points += 1 / (2 ** i)
    parenetp = row['parenetp']
return points
$$
LANGUAGE plpython3u;
