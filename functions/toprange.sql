-- 几周期内最高值
CREATE OR REPLACE FUNCTION toprange(a NUMERIC [])
  RETURNS INT AS $$
# 全不为None
if len(list(filter(lambda x: x is None, a))) > 0:
    plpy.info('Cannot calculate TOPRANGE due to some data are none.')
    return None
# 与前一比我大的值，中间隔了几个
result = len(a) - 1
for i in range(len(a) - 2, -1, -1):
    if a[i] >= a[-1]:
        result = len(a) - i - 2
        break
# 返回个数
return result
$$
LANGUAGE plpython3u;