-- 动态移动平均
CREATE OR REPLACE FUNCTION dma(a NUMERIC [], b NUMERIC [])
  RETURNS NUMERIC AS $$
# 校验参数
if len(a) != len(b):
    plpy.info('Array size mismatch.')
    return None
if len(list(filter(lambda x: x is None, a))) > 0 or len(list(filter(lambda x: x is None, b))) > 0:
    plpy.info('Cannot calculate DMA due to some data are none.')
    return None
# 返回值载体
y = [None for i in range(len(a))]
# 逐一计算
for i in range(0, len(y)):
    y1 = y[i - 1] if i >= 1 else a[i]
    y[i] = b[i] * a[i] + (1 - b[i]) * y1
# 载体最后一个值
return y[-1]
$$
LANGUAGE plpython3u;

SELECT dma(ARRAY [1, 2, 3, 4, 5, 6], ARRAY [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]);