import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras as extras
import streamlit as st
from dateutil.parser import parse
from pandas import ExcelFile
from psycopg2 import sql

# 整型常量定义
SMALLINT_MIN, SMALLINT_MAX = -32768, 32767
INTEGER_MIN, INTEGER_MAX = -2147483648, 2147483647
BIGINT_MIN, BIGINT_MAX = -9223372036854775808, 9223372036854775807


def sanitize_identifier(name, max_length=63):
    """清洗列名/表名"""
    return re.sub(r'[^a-zA-Z0-9]+', '_', name.strip()).strip('_')[:max_length].lower()


import pandas as pd

# ======================
# 整型数据类型取值范围（PostgreSQL 风格）
# ======================
SMALLINT_MIN, SMALLINT_MAX = -32768, 32767
INTEGER_MIN, INTEGER_MAX = -2147483648, 2147483647
BIGINT_MIN, BIGINT_MAX = -9223372036854775808, 9223372036854775807


def infer_sql_type(samples):
    """
    根据样本数据推断 SQL 数据类型：
    - 如果超过 90% 是数字且全部为整数，则尝试识别为 SMALLINT / INTEGER / BIGINT / NUMERIC(38)
    - 如果超过 90% 是数字但包含小数，则识别为 FLOAT
    - 否则识别为 VARCHAR 或其他非数值类型
    """
    # 尝试转换为数字，无法转换的变为 NaN
    numeric_series = pd.to_numeric(samples, errors='coerce')

    # 计算有效数字数量
    notna_count = numeric_series.notna().sum()
    total_count = len(numeric_series)

    # 如果不足 90% 为数字，视为非数值列
    if notna_count / total_count < 0.9:
        return "TEXT"

    # 检查是否所有数字都是整数
    is_all_integer = (numeric_series[numeric_series.notna()] % 1 == 0).all()

    if is_all_integer:
        int_series = numeric_series[numeric_series.notna()].astype(int)
        min_val = int_series.min()
        max_val = int_series.max()
        #
        # 检查是否超出 BIGINT 范围
        if min_val < BIGINT_MIN or max_val > BIGINT_MAX:
            return "NUMERIC(38)"

        # 按照 SMALLINT → INTEGER → BIGINT 的顺序判断最小匹配类型
        if SMALLINT_MIN <= min_val <= SMALLINT_MAX and max_val <= SMALLINT_MAX:
            return "SMALLINT"
        elif INTEGER_MIN <= min_val <= INTEGER_MAX and max_val <= INTEGER_MAX:
            return "INTEGER"
        else:
            return "BIGINT"
    else:
        return "FLOAT"


def infer_column_type(samples):
    samples = samples.dropna()
    if samples.empty:
        return "TEXT"

    # 1. 检查是否是布尔类型
    if all(str(val) in {'true', 'false'} for val in samples.astype(str).str.strip().str.lower().unique()):
        return "BOOLEAN"

    # 2. 检查是否是日期类型
    date_format = detect_date_format(samples)
    if date_format:
        try:
            pd.to_datetime(samples, format=date_format, errors='coerce').dropna()
            return "DATE"
        except:
            pass

    # 3. 尝试识别为整数或浮点数
    data_type = infer_sql_type(samples)
    return data_type


def detect_date_format(samples):
    samples = samples.astype(str).dropna()
    if samples.empty:
        return None

    candidate_formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%Y%m%d",  # 无分隔符
        "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
        "%m-%d-%Y", "%m/%d/%Y", "%m.%d.%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y.%m.%d %H:%M:%S",
        "%Y%m%d%H%M%S"  # 例如 20250405143000
    ]

    matched_formats = []

    for s in samples:
        try:
            dt = parse(s)
            for fmt in candidate_formats:
                if dt.strftime(fmt) == s:
                    matched_formats.append(fmt)
                    break
        except Exception as e:
            continue

    if matched_formats:
        # 返回最常见的格式
        return Counter(matched_formats).most_common(1)[0][0]
    return None


def process_sheet(args):
    """
    单个 sheet 处理函数，用于并发执行
    """
    sheet, xls, schema_dict, table_name, conn_info, chunksize = args
    result = {"sheet": sheet, "success": False, "error": "", "table_name": table_name}

    try:
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()

        # 分块读取支持（适用于 .xlsx）
        data_frame = pd.read_excel(xls, sheet_name=sheet, nrows=10)
        # 清洗样本：统一处理空值和无效值
        data_frame = data_frame.replace(['', '-', 'n/a', 'N/A', 'null', 'NULL', 'None'], pd.NA)

        columns = []
        column_types = []

        for col in data_frame.columns:
            # print("\n")
            safe_col = sanitize_identifier(col)
            # print(f"表名:{table_name}，列名:{safe_col}")
            # print("示例数据:", data_frame[col].iloc[0])

            data_type = infer_column_type(data_frame[col])
            col_type = schema_dict.get(col, data_type)

            # print("实际 dtype:", data_frame[col].dtype)
            # print("推断类型:", data_type)

            columns.append(safe_col)
            column_types.append(col_type)

        # 创建表
        create_table_sql = sql.SQL("DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({fields})").format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join([
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in zip(columns, column_types)
            ])
        )
        cur.execute(create_table_sql)

        # print(cur.query.decode())  # 打印最终执行的 SQL 语句（适用于 psycopg2）

        def clean_tuple(t):
            def replace_nan(x):
                if isinstance(x, float) and pd.isna(x):
                    return None
                elif x is pd.NA:
                    return None
                elif isinstance(x, str) and x.strip().lower() in {'', ' ', 'nan', 'n/a', '-', 'null'}:
                    return None
                return x

            return tuple(replace_nan(x) for x in t)

        # 插入数据
        list_tuples = list(data_frame.itertuples(index=False, name=None))
        tuples = [clean_tuple(t) for t in list_tuples]
        print(tuples)

        placeholders = ", ".join(["%s"] * len(data_frame.columns))
        insert_sql = sql.SQL("INSERT INTO {table} VALUES ({values})").format(
            table=sql.Identifier(table_name),
            values=sql.SQL(placeholders)
        )
        extras.execute_batch(cur, insert_sql, tuples)
        # print(cur.query.decode())

        conn.commit()
        cur.close()
        conn.close()

        result["success"] = True
        result["columns"] = list(zip(columns, column_types))
        result["total_rows"] = len(data_frame)

    except Exception as e:
        result["error"] = str(e)

    return result


def excel_to_db(file, sheet_names, source_database, max_workers=4, chunksize=None):
    import_successful = False
    table_info_list = []

    try:
        # 加载 Excel 文件
        xls = ExcelFile(file)

        # 加载 schema 表
        schema_df = pd.read_excel(xls, sheet_name=sheet_names[0])
        schema_dict = dict(zip(schema_df['col_name'], schema_df['data_type']))

        # 数据库连接参数
        conn_info = {
            "host": source_database['host'],
            "port": source_database['port'],
            "database": source_database['name'],
            "user": source_database['user'],
            "password": source_database['password']
        }

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, sheet in enumerate(sheet_names[1:]):
                raw_table_name = str(sheet).strip()
                table_name = sanitize_identifier(raw_table_name)
                args = (sheet, xls, schema_dict, table_name, conn_info, chunksize)
                future = executor.submit(process_sheet, args)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                if result["success"]:
                    st.success(f"✅ Sheet '{result['sheet']}' 已导入表 '{result['table_name']}'")
                    table_info_list.append({
                        "table_name": result["table_name"],
                        "columns": result["columns"],
                        "raw_sheet_name": result["sheet"],
                        "total_rows": result["total_rows"]
                    })
                else:
                    st.warning(f"⚠️ Sheet '{result['sheet']}' 导入失败: {result['error']}")

        import_successful = True

    except Exception as e:
        print(e)
        st.error(f"❌ 导入失败：{str(e)}")

    return import_successful, table_info_list
