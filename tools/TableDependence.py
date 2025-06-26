import json
import streamlit as st
import pandas as pd
from streamlit_modal import Modal

from get_db_statistic import connect_to_db, get_unique_constraints
from tools.DataConfigManager import DataConfigManager


class TableConfigurator:
    def __init__(self, tables, field_info):
        self.tables = tables
        self.field_info = field_info

        # 初始化默认配置
        default_config = {
            table: {
                "primary_key": [],
                "dep_table": "",
                # "dep_relation": "",
                "dependencies": {
                    "": {
                        "field": "",
                        "func": ""
                    }
                }
            } for table in self.tables
        }

        if 'config' not in st.session_state:
            st.session_state.config = default_config.copy()

    def get_initial_table_data(self, table_name):
        df_fields = pd.DataFrame(self.field_info[table_name])
        config = st.session_state.config[table_name]

        data = []
        for _, row in df_fields.iterrows():
            field_name = row["字段名"]

            fk_info = config["dependencies"].get(field_name, {})
            fk_field = fk_info.get("field", "")

            fk_table = config["dep_table"]

            data.append({
                "字段名": field_name,
                "类型": row["类型"],
                "外键关联表": fk_table,
                "外键字段": fk_field,
                "唯一索引": field_name in config["primary_key"]
            })
        return pd.DataFrame(data)

    def save_configuration(self, table_name, df, source_db: dict):
        # primary_key = df[df["唯一索引"]]["字段名"].tolist()
        primary_key = ""

        dependencies = {}
        dep_relation = "1:5"

        dep_table = ""
        dep_table_col = ""

        src_table_col = ""
        for _, row in df.iterrows():
            fk_table = row["外键关联表"]
            fk_field = row["外键字段"]
            uk_field = row["唯一索引"]
            r_field = row["字段名"]

            if fk_table and fk_field:
                dep_table = fk_table
                dep_table_col = fk_field
                src_table_col = r_field
                dependencies[src_table_col] = {"field": dep_table_col, "func": ""}
            elif uk_field:
                primary_key = r_field
                src_table_col = r_field

        st.session_state.config[table_name]["primary_key"] = primary_key
        st.session_state.config[table_name]["dependencies"] = dependencies
        st.session_state.config[table_name]["dep_table"] = dep_table
        # st.session_state.config[table_name]["dep_relation"] = dep_relation

        con = DataConfigManager(source_db)
        con.test_connection()
        if primary_key:
            # table_name: str, column_name
            # con.add_unique_index_if_not_exists(table_name,src_table_col)
            pass
        elif dep_table and dep_table_col:
            # con.add_foreign_key(table_name, src_table_col, dep_table, dep_table_col)
            pass

    def save_all_to_file(self, filename="dependency.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(st.session_state.config, f, indent=4, ensure_ascii=False)
        return filename

    def display_card_for_field(self, row, table_name, index):
        field_name = row["字段名"]
        key_prefix = f"{table_name}_{field_name}"

        with st.expander(f"🧩 字段：{field_name} ({row['类型']})", expanded=False):
            # 唯一索引
            is_primary_key = st.checkbox("唯一索引", value=row["唯一索引"], key=f"{key_prefix}_is_pk")
            col1, col2 = st.columns(2)

            with col1:
                # 外键关联表
                fk_table = st.selectbox(
                    "外键关联表",
                    options=[""] + self.tables,
                    index=0 if pd.isna(row["外键关联表"]) else self.tables.index(row["外键关联表"]) + 1
                    if row["外键关联表"] in self.tables else 0,
                    key=f"{key_prefix}_fk_table"
                )

            with col2:
                # 动态生成外键字段下拉
                fk_field_options = [""]
                if fk_table and fk_table in self.field_info:
                    fk_field_options += [f["字段名"] for f in self.field_info[fk_table]]

                fk_field = st.selectbox(
                    "外键字段",
                    options=fk_field_options,
                    index=0 if pd.isna(row["外键字段"]) or row["外键字段"] not in fk_field_options
                    else fk_field_options.index(row["外键字段"]),
                    key=f"{key_prefix}_fk_field"
                )

            return {
                "字段名": field_name,
                "类型": row["类型"],
                "外键关联表": fk_table,
                "外键字段": fk_field,
                "唯一索引": is_primary_key
            }
        return None

    def render_main_page(self, source_db: dict):
        # 状态跟踪：记录当前选择的表
        if "last_selected_table" not in st.session_state:
            st.session_state.last_selected_table = None

        col1, col2 = st.columns([4, 1], vertical_alignment="bottom")

        with col1:
            selected_table = st.selectbox(
                "🔍 请选择要配置的表",
                options=self.tables,
                key="main_table_selector"
            )

        with col2:
            if st.button("📁 保存所有表", use_container_width=True):
                file_path = self.save_all_to_file()
                st.toast(f"✅ 所有配置已保存到 `{file_path}`", icon='📄')

        st.divider()

        # 如果切换了表，则清空 updated_rows
        if selected_table != st.session_state.last_selected_table:
            st.session_state.last_selected_table = selected_table
            st.session_state.updated_rows = []

        search_query = st.text_input("🔎 搜索字段名", placeholder="输入字段名进行筛选...")

        tab_fields, tab_config = st.tabs(["🧱 字段管理", "👀 当前配置"])

        with tab_fields:
            if selected_table:
                df = self.get_initial_table_data(selected_table)

                # 根据搜索条件过滤字段
                filtered_df = df[df["字段名"].str.contains(search_query, case=False, na=True)]

                if filtered_df.empty:
                    st.info("ℹ️ 没有匹配的字段。请尝试其他关键词。")
                    return

                # 获取或初始化 updated_rows
                if "updated_rows" not in st.session_state or not isinstance(st.session_state.updated_rows, list):
                    st.session_state.updated_rows = []

                st.session_state.updated_rows.clear()

                # 卡片式展示字段
                st.markdown('<div class="scroll-container">', unsafe_allow_html=True)
                for i, row in filtered_df.iterrows():
                    result = self.display_card_for_field(row, selected_table, i)
                    if result:
                        st.session_state.updated_rows.append(result)
                st.markdown('</div>', unsafe_allow_html=True)

                # 保存当前表按钮
                if st.session_state.updated_rows:
                    if st.button("✅ 保存当前表配置", use_container_width=True, type="primary"):
                        new_df = pd.DataFrame(st.session_state.updated_rows)
                        self.save_configuration(selected_table, new_df, source_db)

                        st.success(f"✅ 表 `{selected_table}` 的配置已保存！")

                else:
                    st.warning("⚠️ 请至少编辑一个字段后再保存配置")

        with tab_config:
            if selected_table:
                st.json(st.session_state.config[selected_table])

    def show_config_modal_after_import(self, source_db: dict):
        modal = Modal("🔧 表结构配置器", key="config_table_modal", padding=20, max_width=1300)
        if st.button("⚙️ 表结构配置", key="config_trigger_btn"):
            modal.open()
        if modal.is_open():
            with modal.container():
                self.render_main_page(source_db)
