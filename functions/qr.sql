-- 速动比率（越小越好）
CREATE OR REPLACE FUNCTION qr(symbol CHAR, date DATE)
  RETURNS NUMERIC AS $$
def cachew(r):
    sql_cachew = 'SELECT cachew($1, $2, $3, $4)'
    plan_cachew = plpy.prepare(sql_cachew, ['CHAR', 'DATE', 'VARCHAR', 'NUMERIC'])
    plpy.execute(plan_cachew, [symbol, date, 'qr', r])
    return r
# 查cache
sql_cacher = 'SELECT yn, val FROM cacher($1, $2, $3)'
plan_cacher = plpy.prepare(sql_cacher, ['CHAR', 'DATE', 'VARCHAR'])
rows_cacher = list(plpy.execute(plan_cacher, [symbol, date, 'qr']))
if rows_cacher[0]['yn']:
    return rows_cacher[0]['val']
# 当前bs.速动资产 and totalcurrliab
# 3月的季报最多用到10月底，6+1个月
# 6月的中报最多用到次年4月底，6+4个月
# 9月的季报最多用到次年4月底，6+1个月
# 12月的季报最多等到明年8月底，6+2个月
sql = 'SELECT "end", curfds, settresedepo, plac, tradfinasset, derifinaasset,  notesrece, accorece, prep, premrece, reinrece, reincontrese, interece, dividrece, otherrece, expotaxrebarece, subsrece, margrece, intelrece, purcresaasset, totalcurrliab FROM balance_sheet WHERE symbol = $1 AND pub <= $2 AND (' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 7 AND (EXTRACT(MONTH FROM "end") = 3)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 10 AND (EXTRACT(MONTH FROM "end") = 6)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 7 AND (EXTRACT(MONTH FROM "end") = 9)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 8 AND (EXTRACT(MONTH FROM "end") = 12))' \
    ') ORDER BY "end" DESC LIMIT 2'
plan = plpy.prepare(sql, ['CHAR', 'DATE'])
rows = list(plpy.execute(plan, [symbol, date]))
if len(rows) < 2:
    plpy.info('Cannot find proper BS statement, sql = %s, $1 = %s, $2 = %s' % (sql, symbol, date))
    return cachew(None)
# 速动资产
quick_asset_0 = rows[0]['curfds'] + rows[0]['settresedepo'] + rows[0]['plac'] + rows[0]['tradfinasset'] + rows[0]['derifinaasset'] + rows[0]['notesrece'] + rows[0]['accorece'] + rows[0]['prep'] + rows[0]['premrece'] + rows[0]['reinrece'] + rows[0]['reincontrese'] + rows[0]['interece'] + rows[0]['dividrece'] + rows[0]['otherrece'] + rows[0]['expotaxrebarece'] + rows[0]['subsrece'] + rows[0]['margrece'] + rows[0]['intelrece'] + rows[0]['purcresaasset']
quick_asset_1 = rows[1]['curfds'] + rows[1]['settresedepo'] + rows[1]['plac'] + rows[1]['tradfinasset'] + rows[1]['derifinaasset'] + rows[1]['notesrece'] + rows[1]['accorece'] + rows[1]['prep'] + rows[1]['premrece'] + rows[1]['reinrece'] + rows[1]['reincontrese'] + rows[1]['interece'] + rows[1]['dividrece'] + rows[1]['otherrece'] + rows[1]['expotaxrebarece'] + rows[1]['subsrece'] + rows[1]['margrece'] + rows[1]['intelrece'] + rows[1]['purcresaasset']
quick_asset = (quick_asset_0 + quick_asset_1) / 2
if quick_asset == 0:
    plpy.info('Quick Asset is zero, symbol = %s, date = %s')
    return cachew(None)
# 流动负债
totalcurrliab = (rows[0]['totalcurrliab'] + rows[1]['totalcurrliab']) / 2
# 计算
return cachew(totalcurrliab / quick_asset)
$$
LANGUAGE plpython3u;