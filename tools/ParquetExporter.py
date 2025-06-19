import json
import pandas as pd
import io
import zipfile
import streamlit as st  # 导入 Streamlit


class ParquetExporter:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.data = None
        self.df = None

    def load_json(self):
        """读取 JSON 文件"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            if hasattr(st, 'success'):
                st.success("✅ 成功加载数据！")
            return True
        except Exception as e:
            if hasattr(st, 'error'):
                st.error(f"❌ 出错了：{e}")
            return False

    def extract_all_tables(self):
        """提取所有子表名称"""
        if self.data is None:
            st.warning("⚠️ 数据尚未加载，请先调用 load_json()。")
            return []

        if isinstance(self.data, dict):
            return list(self.data.keys())
        else:
            st.warning("⚠️ JSON 根层级不是字典，无法识别多个子表。")
            return []

    def extract_table_data(self, table_name):
        """提取指定子表数据并转换为 DataFrame"""
        if table_name not in self.data:
            st.error(f"❌ 子表 '{table_name}' 不存在。")
            return None

        try:
            df = pd.DataFrame.from_records(self.data[table_name])
            return df
        except Exception as e:
            st.error(f"❌ 提取子表 '{table_name}' 出错：{e}")
            return None

    def generate_zip_buffer(self):
        """生成包含所有 Parquet 文件的 ZIP 缓冲区"""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for table_name in self.extract_all_tables():
                df = self.extract_table_data(table_name)
                if df is not None and not df.empty:
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, engine='pyarrow')
                    parquet_buffer.seek(0)

                    zip_filename = f"{table_name}.parquet"
                    zf.writestr(zip_filename, parquet_buffer.getvalue())

        buffer.seek(0)
        return buffer

    def download_zip_button(self, file_name="all_tables_exported.zip"):
        """添加 ZIP 下载按钮（需要在按钮回调中调用）"""
        buffer = self.generate_zip_buffer()
        if buffer:
            st.download_button(
                label="📥 下载 ZIP 文件（含所有 Parquet 表）",
                data=buffer,
                file_name=file_name,
                mime="application/zip"
            )
