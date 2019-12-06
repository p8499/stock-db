-- 低值名次
CREATE OR REPLACE FUNCTION lod(a NUMERIC [], b INT)
  RETURNS INT AS $$
# 子集
sub = a[-b:] if b < len(a) else a
if len(list(filter(lambda x: x is None, sub))) > 0:
    plpy.info('Cannot calculate HOD due to some data are none.')
    return None
# 计数
count = 0
for s in sub:
    if s < sub[-1]:
        count += 1
# 返回排位
return count + 1
$$
LANGUAGE plpython3u;

SELECT lod(ARRAY [5, 4, 6, 5], 3);