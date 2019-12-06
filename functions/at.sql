-- 总资产周转率
CREATE OR REPLACE FUNCTION at(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'at', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'at']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 当前bs.totasset
# 3月的季报最多用到10月底，6+1个月
# 6月的中报最多用到次年4月底，6+4个月
# 9月的季报最多用到次年4月底，6+1个月
# 12月的季报最多等到明年8月底，6+2个月
sql_bs = 'SELECT "end", totasset FROM balance_sheet WHERE symbol = $1 AND pub <= $2 AND (' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 7 AND (EXTRACT(MONTH FROM "end") = 3)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 10 AND (EXTRACT(MONTH FROM "end") = 6)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 7 AND (EXTRACT(MONTH FROM "end") = 9)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 8 AND (EXTRACT(MONTH FROM "end") = 12))' \
    ') ORDER BY "end" DESC LIMIT 2'
plan_bs = plpy.prepare(sql_bs, ['CHAR', 'DATE'])
rows_bs = list(plpy.execute(plan_bs, [symbol, date]))
if len(rows_bs) < 2:
    plpy.info('Cannot find proper BS statement, sql = %s, $1 = %s, $2 = %s' % (sql_bs, symbol, date))
    return cachew(None)
# 资产
totasset = (rows_bs[0]['totasset'] + rows_bs[1]['totasset']) / 2
if totasset == 0:
    plpy.info('Asset is zero, symbol = %s, date = %s')
    return cachew(None)
# 营业总收入
sql_pl = 'SELECT plforecast($1, $2, $3)'
plan_pl = plpy.prepare(sql_pl, ['CHAR', 'DATE', 'CHAR'])
rows_pl = list(plpy.execute(plan_pl, [symbol, date, 'biztotinco']))
biztotinco = rows_pl[0]['plforecast']
if biztotinco is None:
    plpy.info('Cannot calculate ROE due to none value of BIZTOTINCO, symbol = %s, date = %s' % (symbol, date))
    return cachew(None)
# 计算
return cachew(biztotinco / totasset)
$$
LANGUAGE plpython3u;