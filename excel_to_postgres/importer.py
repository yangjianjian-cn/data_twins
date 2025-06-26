# importer.py

import pandas as pd
import psycopg2
import psycopg2.extras as extras
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from utils import infer_column_type, sanitize_identifier


def process_sheet(args):
    sheet_name, xls_bytes, schema_dict, table_name, conn_info = args
    result = {"sheet": sheet_name, "success": False, "error": "", "table_name": table_name}

    try:
        conn = psycopg2.connect(**conn_info)
        cur = conn.cursor()

        # 读取 Excel
        xls = pd.ExcelFile(BytesIO(xls_bytes))
        df = pd.read_excel(xls, sheet_name=sheet_name)

        columns = []
        column_types = []

        for col in df.columns:
            safe_col = sanitize_identifier(col)
            col_type = schema_dict.get(col, infer_column_type(df[col]))
            columns.append(safe_col)
            column_types.append(col_type)

        # 创建表
        create_table_sql = f"""
            DROP TABLE IF EXISTS "{table_name}";
            CREATE TABLE "{table_name}" (
                {", ".join(f'"{c}" {t}' for c, t in zip(columns, column_types))}
            )
        """
        cur.execute(create_table_sql)

        # 插入数据
        tuples = list(df.itertuples(index=False, name=None))
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
        extras.execute_batch(cur, insert_sql, tuples, page_size=100)

        conn.commit()
        cur.close()
        conn.close()

        result["success"] = True
        result["columns"] = list(zip(columns, column_types))
        result["total_rows"] = len(df)

    except Exception as e:
        result["error"] = str(e)

    return result


def excel_to_db(file_bytes, sheet_names, source_database, max_workers=4):
    results = []
    import_successful = False
    table_info_list = []

    try:
        # 加载 Excel 文件
        xls = pd.ExcelFile(file_bytes)

        # 读取 schema 表
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

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, sheet in enumerate(sheet_names[1:]):
                raw_table_name = str(sheet).strip()
                table_name = sanitize_identifier(raw_table_name)
                args = (sheet, file_bytes.getvalue(), schema_dict, table_name, conn_info)
                future = executor.submit(process_sheet, args)
                futures.append(future)

            for future in futures:
                result = future.result()
                results.append(result)

        import_successful = True
        for result in results:
            if result["success"]:
                table_info_list.append({
                    "table_name": result["table_name"],
                    "columns": result["columns"],
                    "raw_sheet_name": result["sheet"],
                    "total_rows": result["total_rows"]
                })

    except Exception as e:
        print("导入错误:", e)

    return import_successful, table_info_list
