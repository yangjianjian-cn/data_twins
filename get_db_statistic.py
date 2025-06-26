import yaml
from psycopg2 import sql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
from datetime import date, datetime
from data_gen import analyze_llm_field


# Add this new class for custom JSON encoding
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config


def connect_to_db(config):
    db_config = config['source_database']
    conn_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
    engine = create_engine(conn_string)

    # 强制清理连接池中的所有连接（确保不会复用旧连接）
    engine.dispose()

    with engine.connect() as conn:
        return engine, conn


def get_tables(engine):
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)['table_name'].tolist()


def get_columns(engine, table):
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table}'
    """
    return pd.read_sql(query, engine).values.tolist()


def get_primary_keys(engine, table):
    query = f"""
    SELECT a.attname
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                         AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '{table}'::regclass
    AND    i.indisprimary
    """
    return pd.read_sql(query, engine)['attname'].tolist()


def get_foreign_keys(engine, table):
    query = f"""
    SELECT
        tc.table_schema, 
        tc.constraint_name, 
        tc.table_name, 
        kcu.column_name, 
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name 
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='{table}'
    """
    return pd.read_sql(query, engine).to_dict('records')


def get_unique_constraints(engine, table):
    query = f"""
    SELECT a.attname
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                         AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = '{table}'::regclass
    AND    i.indisunique
    AND    NOT i.indisprimary
    """
    return pd.read_sql(query, engine)['attname'].tolist()


def calculate_null_rate(df, column):
    total_count = len(df)
    if total_count == 0:
        return 1.0  # 如果总数为0，返回1.0表示100%为空
    null_count = df[column].isnull().sum()
    empty_string_count = 0
    whitespace_count = 0

    if df[column].dtype == 'object':
        empty_string_count = (df[column] == '').sum()
        # 只对非空且为字符串类型的值进行空白检查
        whitespace_count = df[column].dropna().apply(lambda x: str(x).isspace() if isinstance(x, str) else False).sum()

    null_rate = (null_count + empty_string_count + whitespace_count) / total_count
    return float(null_rate)


def analyze_numeric(engine, table, column):
    query = f"SELECT {column} FROM {table}"
    df = pd.read_sql(query, engine)
    null_rate = calculate_null_rate(df, column)
    df_not_null = df[df[column].notnull()]
    if df_not_null.empty:
        return {"stats": {"error": "No non-null values found"}, "null_rate": null_rate, "sample_data": []}
    desc = df_not_null[column].describe()
    return {
        "stats": {
            "mean": float(desc.loc['mean']),
            "min": float(desc.loc['min']),
            "max": float(desc.loc['max'])
        },
        "null_rate": null_rate,
        "sample_data": df_not_null[column].sample(3, replace=True).tolist()
    }


def analyze_character(engine, table, column):
    query = f"SELECT {column} FROM {table}"
    df = pd.read_sql(query, engine)
    null_rate = calculate_null_rate(df, column)
    df_not_null = df[df[column].notnull() & (df[column] != '') & (df[column].str.strip() != '')]
    if df_not_null.empty:
        return {"stats": {"error": "No non-null values found"}, "null_rate": null_rate, "sample_data": []}
    value_counts = df_not_null[column].value_counts(normalize=True)
    return {
        "stats": {str(k): float(v) for k, v in dict(value_counts.nlargest(10)).items()},
        "null_rate": null_rate,
        "sample_data": df_not_null[column].sample(3, replace=True).tolist()
    }


def analyze_date(engine, table, column):
    query = f"SELECT {column} FROM {table}"
    df = pd.read_sql(query, engine)
    null_rate = calculate_null_rate(df, column)
    df_not_null = df[df[column].notnull()]
    if df_not_null.empty:
        return {"stats": {"error": "No non-null values found"}, "null_rate": null_rate, "sample_data": []}
    return {
        "stats": {
            "min_date": str(df_not_null[column].min()),
            "max_date": str(df_not_null[column].max())
        },
        "null_rate": null_rate,
        "sample_data": [str(date) for date in df_not_null[column].sample(3, replace=True).tolist()]
    }


def analyze_long_text(engine, table, column):
    query = f"SELECT {column} FROM {table}"
    df = pd.read_sql(query, engine)
    null_rate = calculate_null_rate(df, column)
    df_not_null = df[df[column].notnull() & (df[column] != '') & (df[column].str.strip() != '')]
    if df_not_null.empty:
        return {"stats": {"error": "No non-null values found"}, "null_rate": null_rate, "sample_data": []}
    lengths = df_not_null[column].str.len()
    return {
        "stats": {
            "min_length": int(lengths.min()),
            "max_length": int(lengths.max()),
            "avg_length": float(lengths.mean())
        },
        "null_rate": null_rate,
        "sample_data": df_not_null[column].sample(3, replace=True).tolist()
    }


def get_codetable_data(engine, table):
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, engine)
    # Convert datetime columns to strings
    for col in df.select_dtypes(include=['datetime64', 'datetime64[ns, UTC]']).columns:
        df[col] = df[col].astype(str)
    return df.to_dict('records')


def get_sample_data(engine, table, column, sample_size=100):
    query = f"SELECT {column} FROM {table} LIMIT {sample_size}"
    return pd.read_sql(query, engine)[column].tolist()


def load_dependency(dependency_file):
    with open(dependency_file, 'r', encoding='utf-8') as file:
        return json.load(file)


def get_db_statistic(config_file='config.yaml', dependency_file='dependency.json'):
    config = load_config(config_file)
    dependency = load_dependency(dependency_file)
    engine, conn = connect_to_db(config)

    tables = get_tables(engine)
    # print("数据库表:", tables)

    codetables = config.get('codetables', [])
    # print("代码表:", codetables)

    specified_columns = config.get('specified_columns', {})
    # print("配置文件指定字段类型:%s", specified_columns)

    result = {}

    for table in tables:
        if table in codetables:
            result[table] = {
                "is_codetable": True,
                "data": get_codetable_data(engine, table)
            }
        else:
            primary_keys = get_primary_keys(engine, table)
            foreign_keys = get_foreign_keys(engine, table)
            unique_constraints = get_unique_constraints(engine, table)
            columns = get_columns(engine, table)
            # print("表的主键:%s", primary_keys)
            # print("表的外键:%s", foreign_keys)
            # print("表的索引:%s", unique_constraints)
            # print("表的列:%s", columns)

            table_stats = {
                "total_rows": int(pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0, 0]),
                "total_columns": len(columns)
            }

            # 添加依赖关系信息
            table_dependency = dependency.get(table, {})
            # print("配置的依赖:%s", table_dependency)

            columns_info = []

            for column, data_type in columns:
                # Check if the column is specified in the YAML file
                specified_type = None
                if table in specified_columns:
                    for col_spec in specified_columns[table]:
                        if column in col_spec:
                            specified_type = col_spec[column]
                            print(f"表中列:{column},有指定类型:{specified_type}")
                            break
                if specified_type == 'llm':
                    # 获取样本数据
                    sample_data = get_sample_data(engine, table, column)
                    # 使用大模型分析字段
                    llm_analysis = analyze_llm_field(table, column, sample_data)
                    if llm_analysis != 'other':
                        columns_info.append({
                            "name": column,
                            "type": llm_analysis,
                            "stats": {"note": "LLM classification result"},
                            "null_rate": None,
                            "sample_data": sample_data[:5],  # 添加样本数据
                            "is_primary_key": column in primary_keys,
                            "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                            "is_unique": column in unique_constraints
                        })
                    else:
                        # 如果是"其他"类型，按照未指定类型处理
                        try:
                            analysis = analyze_column(engine, table, column, data_type)
                            columns_info.append({
                                "name": column,
                                "type": data_type,
                                "stats": analysis["stats"],
                                "null_rate": analysis["null_rate"],
                                "sample_data": analysis["sample_data"],
                                "is_primary_key": column in primary_keys,
                                "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                                "is_unique": column in unique_constraints
                            })
                        except Exception as e:
                            columns_info.append({
                                "name": column,
                                "type": data_type,
                                "stats": {"error": str(e)},
                                "null_rate": None,
                                "sample_data": [],
                                "is_primary_key": column in primary_keys,
                                "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                                "is_unique": column in unique_constraints
                            })
                elif specified_type == 'llm_gen':
                    # 由大模型分析字段，生成数据
                    sample_data = get_sample_data(engine, table, column)
                    columns_info.append({
                        "name": column,
                        "type": specified_type,
                        "stats": {"note": "LLM will generates data for this column."},
                        "null_rate": None,
                        "sample_data": sample_data[:5],  # 添加样本数据
                        "is_primary_key": column in primary_keys,
                        "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                        "is_unique": column in unique_constraints
                    })
                elif specified_type:
                    if data_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
                        # 处理日期时间类型的列
                        analysis = analyze_date(engine, table, column)
                        columns_info.append({
                            "name": column,
                            "type": specified_type,
                            "stats": analysis["stats"],
                            "null_rate": analysis["null_rate"],
                            "sample_data": analysis["sample_data"],
                            "is_primary_key": column in primary_keys,
                            "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                            "is_unique": column in unique_constraints
                        })
                    else:
                        columns_info.append({
                            "name": column,
                            "type": specified_type,
                            "stats": {"note": "Type specified in config.yaml"},
                            "null_rate": None,
                            "sample_data": [],
                            "is_primary_key": column in primary_keys,
                            "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                            "is_unique": column in unique_constraints
                        })
                else:
                    # 处理未指定类型的列
                    try:
                        analysis = analyze_column(engine, table, column, data_type)
                        columns_info.append({
                            "name": column,
                            "type": data_type,
                            "stats": analysis["stats"],
                            "null_rate": analysis["null_rate"],
                            "sample_data": analysis["sample_data"],
                            "is_primary_key": column in primary_keys,
                            "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                            "is_unique": column in unique_constraints
                        })
                    except Exception as e:
                        columns_info.append({
                            "name": column,
                            "type": data_type,
                            "stats": {"error": str(e)},
                            "null_rate": None,
                            "sample_data": [],
                            "is_primary_key": column in primary_keys,
                            "foreign_key": next((fk for fk in foreign_keys if fk['column_name'] == column), None),
                            "is_unique": column in unique_constraints
                        })

            result[table] = {
                "is_codetable": False,
                "table_stats": table_stats,
                "dependency": table_dependency,
                "columns": columns_info
            }

    conn.close()
    engine.dispose()

    # Use the custom encoder when dumping to JSON
    # print(json.dumps(result, indent=2, ensure_ascii=False, cls=DateTimeEncoder))

    # Dump JSON to file using the custom encoder
    with open('db_stats.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)


def analyze_column(engine, table, column, data_type):
    if data_type in ('integer', 'numeric', 'real', 'double precision', 'bigint'):
        return analyze_numeric(engine, table, column)
    elif data_type in ('character', 'character varying'):
        return analyze_character(engine, table, column)
    elif data_type == 'text':
        return analyze_long_text(engine, table, column)
    elif data_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
        return analyze_date(engine, table, column)
    else:
        return {"stats": {"error": f"Unsupported data type: {data_type}"}, "null_rate": None, "sample_data": []}


if __name__ == "__main__":
    # get_db_statistic("test.yaml")
    get_db_statistic("config_local.yaml", "dependency.json")
