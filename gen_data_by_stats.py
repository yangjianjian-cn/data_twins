import json
import re
import random
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import networkx as nx
from faker import Faker
import logging
from data_gen import generate_data_with_llm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

fake = Faker('zh_CN')


def convert_to_date(input):
    if isinstance(input, datetime):
        return input
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y年%m月%d日",
        "%Y年%m月%d日 %H:%M:%S"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(input, fmt)
        except ValueError:
            continue
    raise ValueError(f"无法识别的日期格式: {input}")


def random_date(start, end):
    output = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    # 将输出转换为字符串，格式为"%Y-%m-%d"
    output = output.strftime("%Y-%m-%d")
    return output


def load_db_stats(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def detect_datetime_format(sample: str) -> str:
    formats = [
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%Y年%m月%d日",
        "%Y年%m月%d日 %H时%M分%S秒",
        "%Y年%m月%d日 %H时%M分",
        "%Y年%m月%d %H时",
        "%Y年%m月%d日",
        "%Y年%m月"
    ]

    for fmt in formats:
        try:
            datetime.strptime(sample, fmt)
            return fmt
        except ValueError:
            continue
    return None


def build_dependency_graph(db_stats):
    G = nx.DiGraph()
    for table, table_info in db_stats.items():
        G.add_node(table)
        # 检查表是否有依赖关系
        if 'dependency' in table_info and 'dep_table' in table_info['dependency']:
            dep_table = table_info['dependency']['dep_table']
            G.add_edge(dep_table, table)
        # 检查列是否有外键关系
        for column in table_info.get('columns', []):
            if column.get('foreign_key'):
                fk_info = column['foreign_key']
                G.add_edge(fk_info['foreign_table_name'], table)
    return G


def topological_sort(G):
    return list(nx.topological_sort(G))


def generate_data(db_stats, sorted_tables, num_records=10):
    code_table_data = {}
    unique_values = defaultdict(set)  # 用于跟踪唯一字段的值

    # 先生成代码表数据
    print("加载代码表")
    for table in sorted_tables:
        if table and db_stats[table].get('is_codetable', False):
            code_table_data[table] = db_stats[table].get('data', [])
            print(f"{table}，共 {len(code_table_data[table])} 条记录")

    all_data = code_table_data.copy()

    print("\n加载非代码表")
    for _ in range(num_records):
        print(f"第{_ + 1}条")
        current_record = code_table_data.copy()
        for table in sorted_tables:
            if not db_stats[table].get('is_codetable', False):
                current_record[table] = []
                # 非代码表生成数据
                generate_table_data(db_stats, table, current_record, unique_values)

        # 将当前记录合并到 all_data, 要跳过 code_table_data 中的数据
        for table, data in current_record.items():
            if table not in code_table_data:
                if table not in all_data:
                    all_data[table] = []
                all_data[table] = all_data[table] + data

    all_data = {k: v for k, v in all_data.items() if k not in code_table_data}
    return all_data


"""
db_stats, table, current_record, unique_values
"""


def generate_table_data(db_stats, table, all_data, unique_values):
    table_info = db_stats[table]
    dependency = table_info.get('dependency', {})
    dep_table = dependency.get('dep_table')
    print(f"表:{table}, 关联表:{dep_table}")

    # del
    if dep_table:
        if dep_table not in all_data or len(all_data[dep_table]) == 0:
            return

        # 依赖表的最后一条记录作为“父记录”
        parent_record = all_data[dep_table][-1]

        # 从配置项中读取类似 "1:3" 的字符串，拆分为最小和最大记录数。
        min_records, max_records = map(int, dependency['dep_relation'].split(':'))
        # 随机决定要生成多少条子记录。
        num_records = random.randint(min_records, max_records)
        print(f"随机生成{num_records}条子记录")

        for _ in range(num_records):
            record = generate_record(db_stats, table, parent_record, dependency, unique_values, all_data)
            if record:
                all_data[table].append(record)
    else:
        # 如果没有依赖表，生成一条记录
        record = generate_record(db_stats, table, None, None, unique_values, all_data)
        if record:
            all_data[table].append(record)


def generate_record(db_stats, table, parent_record, dependency, unique_values, all_data):
    record = {}
    table_info = db_stats[table]

    for column in table_info['columns']:
        column_name = column['name']
        # del
        if dependency and parent_record and column_name in dependency.get('dependencies', {}):
            dep_info = dependency['dependencies'][column_name]
            parent_field = dep_info['field']
            func = dep_info['func']
            if func:
                # 执行自定义函数
                value = eval(func)(parent_record[parent_field])
            else:
                # 父记录中关联字段的值，作为子记录字段的值
                value = parent_record[parent_field]
            print(f"字段:{column_name},关联字段:{parent_field},值:{value}")
        else:
            value = generate_column_data(table, column, unique_values, all_data)
        if value is None:
            return None  # 如果无法生成唯一值，则放弃整个记录

        record[column_name] = value

    return record


def generate_column_data(table, column, unique_values, all_data):
    # print(f"正在生成表 {table} 列 {column['name']},类型为:{column['type']} 的数据")
    # 外键
    if 'foreign_key' in column:
        fk_info = column['foreign_key']
        if fk_info is not None:
            foreign_table = fk_info['foreign_table_name']
            foreign_column = fk_info['foreign_column_name']
            if foreign_table in all_data and all_data[foreign_table]:
                foreign_column_value = random.choice(all_data[foreign_table])[foreign_column]
                print(
                    f"字段 {column['name']},关联表 {foreign_table},关联字段 {foreign_column},值(父字段随机一条) {foreign_column_value}")
                return foreign_column_value
            else:
                return None  # 如果外键表还没有数据，返回 None

    # is_unique = column.get('is_unique', False) or column.get('is_primary_key', False)
    is_primary = column.get('is_primary_key', False)
    # 主键
    if is_primary:
        max_attempts = 100  # 最大尝试次数
        primary_key = f"{table}.{column['name']}"
        print("主键:", primary_key)

        for _ in range(max_attempts):
            primary_value = generate_unique_data(column, unique_values.get(primary_key, set()))
            if primary_value is not None and primary_value not in unique_values.get(primary_key, set()):
                if primary_key not in unique_values:
                    unique_values[primary_key] = set()
                unique_values[primary_key].add(primary_value)
                return primary_value
        return None  # 如果无法生成唯一值，则返回 None

    code_key = all_data.get(column['name'])
    if code_key:
        options = all_data[column['name']]
        code_value = random.choice(options)["value"]
        print(f"字段 {column['name']},其值取自代码表:{code_value}")
        return code_value
    else:
        # 其它列
        other_column_val = generate_single_column_data(column)
        return other_column_val


def generate_unique_data(column, existing_values):
    column_type = column['type']

    if column_type in ('integer', 'bigint'):
        return random.randint(0, 1000000)
    elif column_type in ('numeric', 'real', 'double precision'):
        return round(random.uniform(0, 1000000), 2)
    elif column_type in ('character', 'character varying', 'text'):
        return fake.uuid4()
    elif column_type in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
        return fake.date_time().isoformat()
    else:
        return None  # 不支持的类型


def generate_single_column_data(column):
    # 这里包含原来 generate_column_data 函数的逻辑
    if column['type'] == 'llm_gen':
        return generate_llm_data(column)

    faker_type = get_faker_type(column)

    if faker_type:
        return generate_faker_data(faker_type, column)

    # print("column_type:",column['type'])
    # 如果不是指定的Faker类型，则按原来的逻辑处理
    if column['type'] == 'boolean':
        return random.choice([True, False])
    elif column['type'].lower() in ('integer', 'bigint', 'smallint'):
        return generate_numeric_data(column, is_integer=True)
    elif column['type'] in ('numeric', 'real', 'double precision'):
        return generate_numeric_data(column, is_integer=False)
    elif column['type'] in ('character', 'character varying'):
        return generate_character_data(column)
    elif column['type'] == 'text':
        # return generate_text_data(column)
        return "未模拟"
    elif column['type'] in ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'):
        return generate_date_data(column)
    else:
        return None  # 不支持的类型


def generate_llm_data(column):
    sample_data = column.get('sample_data', [])
    if not sample_data:
        return None

    # Generate 20 items using LLM
    generated_data = generate_data_with_llm(sample_data, 20)

    # If we need more than 20 items, randomly sample from the generated data
    if len(generated_data) < len(sample_data):
        return random.choice(generated_data)
    else:
        return random.choice(sample_data)


def get_faker_type(column):
    if column.get('stats').get('note') in ["Type specified in config.yaml", "LLM classification result"]:
        return column.get('type')
    return get_faker_method(column.get('sample_data', []))


def generate_faker_data(faker_type, column):
    if faker_type in ['date', 'time', 'date_time']:
        sample_format = get_sample_format(column)
        if sample_format:
            # 读取stat中的最大最小值
            stats = column.get('stats', {})
            min_date = parse_date(stats.get('min_date', '-30y'))
            max_date = parse_date(stats.get('max_date', 'now'))
            return fake.date_time_between(start_date=min_date, end_date=max_date).strftime(sample_format)
    return getattr(fake, faker_type)()


def get_sample_format(column):
    if 'sample_data' in column:
        for sample in column['sample_data']:
            sample_format = detect_datetime_format(str(sample))
            if sample_format:
                return sample_format
    # 如果没有抽样或侦测失败，返回缺省格式
    return "%Y-%m-%d"


def get_faker_method(sample_data):
    if not sample_data or not isinstance(sample_data[0], str):
        return None

    # 初始化计数器
    method_counter = Counter()

    for sample in sample_data:
        sample = str(sample)

        # Email 判断
        if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', sample):
            method_counter['email'] += 1

        # 地址判断
        if any(word in sample for word in ['省', '市', '区', '县', '街', '路', '道', '巷']):
            method_counter['address'] += 1

        # URL 判断
        if re.match(r'^(http|https)://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}(?:/\S*)?$', sample.lower()):
            method_counter['url'] += 1

        # 姓名判断
        # if 2 <= len(sample) <= 4 and all('\u4e00' <= char <= '\u9fff' for char in sample):
        #     method_counter['name'] += 1

        # 手机号判断
        if re.match(r'^1[3-9]\d{9}$', sample):
            method_counter['phone_number'] += 1

        # # 身份证号判断, faker 不支持，以再增加
        # if re.match(r'^\d{17}[\dXx]$', sample):
        #     method_counter['id_card'] += 1

        # 日期判断
        if re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$', sample):
            method_counter['date'] += 1

    # 如果匹配到任何类型，返回 None
    if not method_counter:
        return None

    # 返回匹配次数最多的类型
    most_common = method_counter.most_common(1)
    if most_common[0][1] / len(sample_data) >= 0.8:  # 如果80%以上的样本匹配某一类型
        return most_common[0][0]
    else:
        return None


def generate_numeric_data(column, is_integer=False):
    stats = column.get('stats', {})
    if stats and 'min' in stats and 'max' in stats:
        min_val = stats.get('min')
        max_val = stats.get('max')
        if min_val is None or max_val is None:
            min_val = 0
            max_val = 1000000 if is_integer else 1000.0
    else:
        min_val = 0
        max_val = 1000000 if is_integer else 1000.0

    if is_integer:
        return random.randint(int(min_val), int(max_val))
    else:
        value = random.uniform(float(min_val), float(max_val))
        return round(value, 2)  # 默认保留两位小数


def generate_character_data(column):
    stats = column.get('stats', {})
    if stats and len(stats) > 0:
        try:
            weights = [float(value) for value in stats.values()]
            return random.choices(list(stats.keys()), weights=weights)[0]
        except ValueError:
            return random.choice(list(stats.keys()))
    else:
        return fake.word()  # 使用Faker生成随机单词


def generate_text_data(column):
    stats = column.get('stats', {})
    if stats and 'min_length' in stats and 'max_length' in stats:
        min_length = max(5, int(stats.get('min_length', 5)))
        max_length = max(min_length, int(stats.get('max_length', 100)))
    else:
        min_length = 5
        max_length = 100

    length = random.randint(min_length, max_length)
    return fake.text(max_nb_chars=length)


def generate_date_data(column):
    print(column)

    stats = column.get('stats', {})
    if stats and 'min_date' in stats and 'max_date' in stats:
        min_date = parse_date(stats.get('min_date', '1970-01-01'))
        max_date = parse_date(stats.get('max_date', datetime.now().strftime('%Y-%m-%d')))
    else:
        min_date = datetime(1970, 1, 1)
        max_date = datetime.now()

    generated_date = fake.date_time_between(start_date=min_date, end_date=max_date)
    # 检测样本数据的格式
    sample_format = get_sample_format(column)

    # 如果检测到格式，使用该格式否则使用默认格式
    if sample_format:
        return generated_date.strftime(sample_format)
    else:
        return generated_date.strftime('%Y-%m-%d')


def parse_date(date_string):
    # 手动去掉时区信息
    if '+' in date_string:
        date_string = date_string.split('+')[0]

    if date_string == '-30y':
        return datetime.now() - timedelta(days=30 * 365)
    elif date_string == 'now':
        return datetime.now()
    formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"无法解析日期: {date_string}")


def gen_data_by_stats(stats_file='db_stats.json', num_records=10):
    db_stats = load_db_stats(stats_file)
    dependency_graph = build_dependency_graph(db_stats)
    sorted_tables = topological_sort(dependency_graph)
    generated_data = generate_data(db_stats, sorted_tables, num_records)

    return generated_data


def save_to_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"数据已保存到 {file_path}")


if __name__ == "__main__":
    generated_data = gen_data_by_stats(num_records=5000)
    if generated_data is not None:
        print(f"总共生成了 {sum(len(data) for data in generated_data.values())} 条记录")
        save_to_json(generated_data, 'db_data.json')
    else:
        print("生成数据失败，请检查错误信息。")
