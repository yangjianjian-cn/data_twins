import pandas as pd

# 测试数据
data = ['20250623', '', '-', 'n/a', 'NULL', None, '2024-02-29', 'N/A', 'normal_data']
s = pd.Series(data)

# 执行清洗和 dropna
cleaned = s.replace(['', '-', 'n/a', 'N/A', 'null', 'NULL', None], pd.NA)
samples = cleaned.dropna()

print("原始数据:")
print(s)

print("\n清洗后的数据（替换为 pd.NA）:")
print(cleaned)

print("\n去除了缺失后的有效数据:")
print(samples)
