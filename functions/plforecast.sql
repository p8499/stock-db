--PL表年度KPI预测，仅能预测正值KPI
CREATE OR REPLACE FUNCTION plforecast(symbol CHAR, date DATE, kpi CHAR)
  RETURNS NUMERIC AS $$
from datetime import datetime
# 当前pl.kpi
# 3月的季报最多用到8月底，3+2个月
# 6月的中报最多用到10月底，3+1个月
# 9月的季报最多用到次年3月底，3+4个月
# 12月的季报最多等到次年4月底，3+1个月
sql_current = 'SELECT "end", %s FROM income_statement WHERE symbol = $1 AND pub <= $2 AND (' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 5 AND (EXTRACT(MONTH FROM "end") = 3)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 4 AND (EXTRACT(MONTH FROM "end") = 6)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 7 AND (EXTRACT(MONTH FROM "end") = 9)) OR ' \
    '(EXTRACT(YEAR FROM $2) * 12 + EXTRACT(MONTH FROM $2) - EXTRACT(YEAR FROM "end") * 12 - EXTRACT(MONTH FROM "end") <= 4 AND (EXTRACT(MONTH FROM "end") = 12))' \
    ') ORDER BY "end" DESC LIMIT 1' % kpi
plan_current = plpy.prepare(sql_current, ['CHAR', 'DATE'])
rows_current = list(plpy.execute(plan_current, [symbol, date]))
if len(rows_current) == 0:
    plpy.info('Cannot find proper PL statement, sql = %s, $1 = %s, $2 = %s' % (sql_current, symbol, date))
    return None
current_kpi = rows_current[0][kpi]
current_end = rows_current[0]['end']
# 如果最新财报为季报，则预测；如为年报，则直接返回
if int(datetime.strptime(current_end, '%Y-%m-%d').strftime('%m')) < 12:
    n = 3
    # n条历史同季度pl.kpi
    sql_history = 'SELECT "end", %s FROM income_statement WHERE symbol = $1 AND "end" < $2 AND EXTRACT(MONTH FROM "end") = EXTRACT(MONTH FROM $2) ORDER BY "end" DESC LIMIT %d' % (kpi, n)
    plan_history = plpy.prepare(sql_history, ['CHAR', 'DATE'])
    rows_history = list(plpy.execute(plan_history, [symbol, current_end]))
    # 每条补全其同年年报pl.kpi
    sql_history_year = 'SELECT %s FROM income_statement WHERE symbol = $1 AND EXTRACT(YEAR FROM "end") = EXTRACT(YEAR FROM $2) AND EXTRACT(MONTH FROM "end") = 12' % kpi
    plan_history_year = plpy.prepare(sql_history_year, ['CHAR', 'DATE'])
    for row in reversed(rows_history):
        rows_history_year = list(plpy.execute(plan_history_year, [symbol, row['end']]))
        if len(rows_history_year) > 0:
            row['year_%s' % kpi] = rows_history_year[0][kpi]
        else:
            rows_history.remove(row)
    # 去除无法计算的行
    for row in reversed(rows_history):
        if row[kpi] <= 0 or row['year_%s' % kpi] <= 0:
            rows_history.remove(row)
    # 补充完年kpi后、去除无法计算的行后，列表不能无数据
    if len(rows_history) == 0:
        plpy.info('Not enough history data to forecast, sql = %s, $1 = %s, $2 = %s' % (sql_history, symbol, current_end))
        return None
    # 平均rate
    rate = sum(map(lambda x: x['year_%s' % kpi] / x[kpi], rows_history)) / len(rows_history)
    # 计算
    return current_kpi * rate
else:
    return current_kpi
$$
LANGUAGE plpython3u;