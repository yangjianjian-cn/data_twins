import yaml
import json
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, PrimaryKeyConstraint, UniqueConstraint, ForeignKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import inspect
from collections import OrderedDict
from sqlalchemy import text
import re

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def drop_all_tables(engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    metadata.drop_all(bind=engine)
    print("All existing tables have been dropped.")

def escape_comment(comment):
    if comment is None:
        return None
    # 将单引号替换为两个单引号，这是SQL中转义单引号的标准方法
    return comment.replace("'", "''")

def clone_database_structure(source_engine, target_engine):
    # 获取源数据库的元数据
    source_metadata = MetaData()
    source_metadata.reflect(bind=source_engine)
    source_metadata.create_all(target_engine)
    # 复制表和字段的注释
    with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
        # 获取所有表名
        tables = source_metadata.tables.keys()
        
        for table_name in tables:
            # 获取表注释
            table_comment = source_conn.execute(text(f"SELECT obj_description('{table_name}'::regclass, 'pg_class') AS comment")).scalar()
            if table_comment:
                target_conn.execute(text(f"COMMENT ON TABLE {table_name} IS '{escape_comment(table_comment)}'"))
            
            # 获取所有列
            columns = source_metadata.tables[table_name].columns
            
            for i, column in enumerate(columns, start=1):
                # 使用枚举索引替代 ordinal_position
                column_comment = source_conn.execute(text(f"SELECT col_description('{table_name}'::regclass::oid, {i}) AS comment")).scalar()
                if column_comment:
                    target_conn.execute(text(f"COMMENT ON COLUMN {table_name}.{column.name} IS '{escape_comment(column_comment)}'"))
        
        print("所有表和字段的注释已成功复制")
    print("All table structures have been successfully cloned")

def insert_data(engine, data):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 使用 OrderedDict 来保持 JSON 文件中的顺序
        from collections import OrderedDict
        ordered_data = OrderedDict(data)
        
        for table_name, table_data in ordered_data.items():
            # 获取表的元数据
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=engine)
            
            inserted_count = 0
            skipped_count = 0
            
            print(f"开始插入表 {table_name} 的数据")
            
            # 插入数据
            for row in table_data:
                try:
                    session.execute(table.insert().values(**row))
                    inserted_count += 1
                except IntegrityError as e:
                    print(f"插入数据到表 {table_name} 时发生完整性错误: {str(e)}")
                    session.rollback()  # 回滚此次插入
                    skipped_count += 1
                    continue  # 跳过此条数据，继续下一条
                except SQLAlchemyError as e:
                    print(f"插入数据到表 {table_name} 时发生错误: {str(e)}")
                    session.rollback()  # 回滚此次插入
                    skipped_count += 1
                    continue  # 跳过此条数据，继续下一条
            
            session.commit()
            print(f"表 {table_name}: 成功插入 {inserted_count} 条记录，跳过 {skipped_count} 条记录")
        
        print("所有数据按顺序插入完成")
    except SQLAlchemyError as e:
        session.rollback()
        print(f"处理数据时发生错误: {str(e)}")
    finally:
        session.close()

def save_data_to_db(source_config, target_config, data_file='db_data.json', drop_existing_tables=False):
    # 创建数据库引擎
    source_engine = create_engine(f"postgresql://{source_config['user']}:{source_config['password']}@{source_config['host']}:{source_config['port']}/{source_config['name']}")
    target_engine = create_engine(f"postgresql://{target_config['user']}:{target_config['password']}@{target_config['host']}:{target_config['port']}/{target_config['name']}")
    
    if drop_existing_tables:
        drop_all_tables(target_engine)
    
    # 克隆数据库结构（包括主键和外键约束）
    clone_database_structure(source_engine, target_engine)
    
    # 读取 JSON 数据，保持顺序
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f, object_pairs_hook=OrderedDict)
    
    # 插入数据
    insert_data(target_engine, data)

if __name__ == "__main__":
    # 读取 YAML 配置文件
    config = load_config('config_local.yaml')
    
    # 获取源数据库和目标数据库的配置
    source_config = config['source_database']
    target_config = config['target_database']
    
    # 可以通过修改这里的参数来控制是否删除现有表
    save_data_to_db(source_config, target_config, drop_existing_tables=True)