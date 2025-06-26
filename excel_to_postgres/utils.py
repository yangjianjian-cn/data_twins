# utils.py

import pandas as pd
import re
from collections import Counter

# 整型常量定义
SMALLINT_MIN, SMALLINT_MAX = -32768, 32767
INTEGER_MIN, INTEGER_MAX = -2147483648, 2147483647
BIGINT_MIN, BIGINT_MAX = -9223372036854775808, 9223372036854775807


def sanitize_identifier(name, max_length=63):
    """清洗列名/表名"""
    return re.sub(r'[^a-zA-Z0-9]+', '_', name.strip()).strip('_')[:max_length].lower()


def detect_date_format(samples):
    samples = samples.astype(str).dropna()
    if samples.empty:
        return None

    formats = []
    for s in samples:
        s = str(s).strip()
        if re.match(r'^\d{4}/\d{1,2}/\d{1,2}$', s):
            formats.append("%Y/%m/%d")
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            formats.append("%Y-%m-%d")
        elif re.match(r'^\d{4}\.\d{2}\.\d{2}$', s):
            formats.append("%Y.%m.%d")
        elif re.match(r'^\d{8}$', s):  # yyyymmdd
            formats.append("%Y%m%d")

    if formats:
        return Counter(formats).most_common(1)[0][0]
    return None


def infer_column_type(series, sample_size=20):
    cleaned = series.head(sample_size).replace(['', '-', 'n/a', 'N/A', 'null', 'NULL', None], pd.NA)
    samples = cleaned.dropna()
    if samples.empty:
        return "TEXT"

    # 布尔判断
    unique_vals = set(samples.astype(str).str.strip().str.lower())
    if all(val in {'true', 'false'} for val in unique_vals):
        return "BOOLEAN"

    # 日期格式检测
    date_format = detect_date_format(samples)
    if date_format:
        try:
            pd.to_datetime(samples, format=date_format, errors='coerce').dropna()
            return "DATE"
        except:
            pass

    # 数字类型检测
    numeric_series = pd.to_numeric(samples, errors='coerce')
    notna_count = numeric_series.notna().sum()
    total_count = len(numeric_series)

    if notna_count / total_count >= 0.9:
        if (numeric_series[numeric_series.notna()] % 1 == 0).all():
            int_series = numeric_series[numeric_series.notna()].astype(int)
            min_val, max_val = int_series.min(), int_series.max()

            if SMALLINT_MIN <= min_val <= SMALLINT_MAX and SMALLINT_MIN <= max_val <= SMALLINT_MAX:
                return "SMALLINT"
            elif INTEGER_MIN <= min_val <= INTEGER_MAX and INTEGER_MIN <= max_val <= INTEGER_MAX:
                return "INTEGER"
            elif BIGINT_MIN <= min_val <= BIGINT_MAX and BIGINT_MIN <= max_val <= BIGINT_MAX:
                return "BIGINT"
            else:
                return "NUMERIC(38)"
        else:
            return "FLOAT"

    return "TEXT"
