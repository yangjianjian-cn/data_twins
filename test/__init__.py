import pandas as pd

# 整型范围定义
SMALLINT_MIN, SMALLINT_MAX = -32768, 32767
INTEGER_MIN, INTEGER_MAX = -2147483648, 2147483647
BIGINT_MIN, BIGINT_MAX = -9223372036854775808, 9223372036854775807


def infer_sql_type(samples):
    numeric_series = pd.to_numeric(samples, errors='coerce')
    notna_count = numeric_series.notna().sum()
    total_count = len(numeric_series)

    if notna_count / total_count < 0.9:
        return "VARCHAR"

    is_all_integer = (numeric_series[numeric_series.notna()] % 1 == 0).all()

    if is_all_integer:
        int_series = numeric_series[numeric_series.notna()].astype(int)

        if int_series.empty:
            return "VARCHAR"

        min_val = int_series.min()
        max_val = int_series.max()

        if min_val < BIGINT_MIN or max_val > BIGINT_MAX:
            return "NUMERIC(38)"
        elif SMALLINT_MIN <= min_val <= SMALLINT_MAX and max_val <= SMALLINT_MAX:
            return "SMALLINT"
        elif INTEGER_MIN <= min_val <= INTEGER_MAX and max_val <= INTEGER_MAX:
            return "INTEGER"
        else:
            return "BIGINT"
    else:
        return "FLOAT"


def main():
    test_cases = [
        (['1', '2', '3'], 'SMALLINT'),
        (['32767', '32766'], 'SMALLINT'),
        (['-32768', '0', '32767'], 'SMALLINT'),
        (['-32769', '0', '32767'], 'INTEGER'),
        (['2147483647', '-2147483648'], 'INTEGER'),
        (['9223372036854775807'], 'BIGINT'),
        (['9223372036854775808'], 'NUMERIC(38)'),
        (['1.5', '2', '3'], 'FLOAT'),
        (['abc', '123'], 'VARCHAR'),
        ([None, None, None], 'VARCHAR'),
        ([], 'VARCHAR'),
        (['1', '2', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc', 'abc'], 'SMALLINT'),  # 9/10 是数字
        (['9223372036854775807', '9223372036854775807'], 'BIGINT'),
    ]

    for i, (samples, expected) in enumerate(test_cases):
        result = infer_sql_type(samples)
        print(f"Test Case {i+1}: {samples} → Result: {result}, Expected: {expected}, Pass: {result == expected}")


if __name__ == '__main__':
    main()
