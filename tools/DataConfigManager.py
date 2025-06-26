from sqlalchemy import create_engine, text
from sqlalchemy.dialects import postgresql


class DataConfigManager:
    def __init__(self, db_config: dict):
        """
        初始化数据库连接引擎
        :param db_config: 包含数据库连接信息的字典
        """
        self.db_config = db_config
        self.engine = self._create_engine()

    def _create_engine(self):
        """
        创建 SQLAlchemy 引擎
        """
        return create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['name']}"
        )

    def add_foreign_key(self, table_name: str, src_column: str, ref_table: str, ref_column: str):
        """
        添加外键约束
        :param table_name: 源表名
        :param src_column: 源字段名
        :param ref_table: 引用表名
        :param ref_column: 引用字段名
        :return: None
        """
        constraint_name = f"{table_name}_{src_column}_fk"

        # 检查是否已经存在该外键约束
        if self._constraint_exists(constraint_name):
            print(f"Constraint {constraint_name} already exists. Skipping.")
            return

        # 构建 SQL 语句
        query = text(f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {constraint_name}
            FOREIGN KEY ({src_column})
            REFERENCES {ref_table} ({ref_column});
        """)

        # 执行 SQL
        try:
            with self.engine.connect() as conn:
                print(query)
                conn.execute(query)
                conn.commit()
            print(f"Foreign key {constraint_name} added successfully.")
        except Exception as e:
            print(f"Error adding foreign key {constraint_name}: {e}")
            raise

    def _constraint_exists(self, constraint_name: str) -> bool:
        """
        检查指定名称的约束是否存在
        """
        check_sql = text(f"""
            SELECT 1 FROM pg_constraint WHERE conname = '{constraint_name}'
        """)
        with self.engine.connect() as conn:
            result = conn.execute(check_sql).fetchone()
        return result is not None

    def index_exists(self, table_name: str, column_name: str) -> bool:
        sql = text(f"""
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = '{table_name}'
              AND indexdef LIKE '%UNIQUE%({column_name})'
        """)

        conn = self.engine.connect()
        try:
            print(sql.compile(dialect=postgresql.dialect()))
            result = conn.execute(sql).fetchone()
            print("查询结果:", result)
            print(result)
        finally:
            conn.close()

        return result is not None

    def add_unique_index_if_not_exists(self, table_name: str, column_name: str):
        """
        如果字段没有唯一索引，则创建唯一索引
        :param table_name: 表名
        :param column_name: 字段名
        :return: None
        """
        index_name = f"{table_name}_{column_name}_ux"
        if self.index_exists(table_name, column_name):
            print(f"Unique index '{index_name}' already exists. Skipping.")
            return

        query = text(f"""
            CREATE UNIQUE INDEX {index_name}
            ON {table_name} ({column_name});
        """)

        try:
            with self.engine.connect() as conn:
                print(query)
                conn.execute(query)
                conn.commit()
            print(f"Unique index '{index_name}' created successfully.")
        except Exception as e:
            print(f"Error creating unique index '{index_name}': {e}")
            conn.rollback()
            raise

    def test_connection(self):
        """
        测试数据库是否连接成功
        :return: True if connected, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # 可以执行一个简单的查询来验证连接可用性，例如获取 PostgreSQL 版本
                result = conn.execute(text("SELECT version();")).fetchone()
                print("Database connection successful.")
                print("PostgreSQL version:", result[0])
                return True
        except Exception as e:
            print("Failed to connect to the database:", str(e))
            return False


# source_db = {'host': '192.168.0.164', 'port': '15432', 'name': 'dwd_pos_t90', 'user': 'data_twins',
#              'password': 'twins2025'}
# con = DataConfigManager(source_db)
# con.add_unique_index_if_not_exists('t90_sample', 'gtin_code')
