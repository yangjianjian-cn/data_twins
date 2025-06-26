import pandas as pd

# 指定Excel文件路径
file_path = r'C:\Users\Administrator\Desktop\数据处理\PG_DWD_POS_T90_Sample_Data.xlsx'

# 只读取需要的三列
columns_to_read = [
    # 1. 产品中文名称与规格
    "fpc_product_name_cn",  # 中文产品名
    "fpc_product_specification",  # 规格
    # "fpc_price_tier",  # 价格层级

    # 2. 产品英文名称与编码
    "fpc_product_name_en",  # 英文产品名（含SKU）

    # 3. 行业分类体系（从宏观到微观）
    "sector",  # 领域
    "sub_sector",  # 子领域
    "category",  # 类别
    "category_code",  # 类别编码
    "category_en",  # 类别英文缩写
    "full_category_en",  # 类别英文全称
    "category_cn",  # 类别中文缩写

    # 4. 品牌体系（主品牌 → 子品牌 → 品牌细分）
    "brand",  # 主品牌
    "brand_code",  # 品牌编码
    "brand_en",  # 品牌英文缩写
    "full_brand_en",  # 品牌英文全称
    "brand_cn",  # 品牌中文缩写
    "sub_brand",  # 子品牌/系列
    "sub_brand_code",  # 子品牌编码
    "sub_brand_en",  # 子品牌英文名
    "sub_brand_cn",  # 子品牌中文名
    "brand_segment",  # 品牌细分（如 Diapers）
    "brand_form",  # 产品形式（如 Taped, Pull-On）
    "brand_element",  # 品牌元素（内部标识）

    # 5. 产品形式与变体（Product Form & Variant）
    "product_form_code",  # 产品形式编码
    "product_form_en",  # 产品形式英文名
    "product_form_cn",  # 产品形式中文名
    "brand_product_form_en",  # 品牌专属的产品形式名称
    "brand_product_form_cn",  # 品牌专属的产品形式中文名称
    "variant_code",  # 变体编码
    "variant_en",  # 变体简称（如 Baby Basic, Pink Ultra）
    "full_variant_en",  # 变体全称（如 帮宝适干爽健康）
    "variant_cn",  # 变体中文简称
    "full_variant_cn"  # 变体中文全称
]

# 读取Excel并筛选出这三列
df = pd.read_excel(file_path, sheet_name='gtin_fpc_dim sample', usecols=columns_to_read)

# 删除所有列都为空的行
df = df.dropna(how='all')

# 去重：保留唯一的 sector - sub_sector - category - brand 组合
unique_df = df.drop_duplicates(subset=columns_to_read)

# 排序：依次按照 sector、sub_sector、category、brand 升序排列
sorted_df = unique_df.sort_values(by=columns_to_read)

# 输出到CSV文件
output_path = r'processed_output.csv'
sorted_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("数据已成功导出至:", output_path)
