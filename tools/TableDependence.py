import json
import streamlit as st
import pandas as pd
from streamlit_modal import Modal


class TableConfigurator:
    def __init__(self, tables, field_info):
        self.tables = tables
        self.field_info = field_info

        # 初始化默认配置
        default_config = {
            table: {
                "primary_key": [],
                "dep_table": "",
                "dep_relation": "",
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
                "是否主键": field_name in config["primary_key"]
            })
        return pd.DataFrame(data)

    def save_configuration(self, table_name, df):
        primary_key = df[df["是否主键"]]["字段名"].tolist()

        dependencies = {}
        dep_relation = "1:5"
        dep_table = ""
        for _, row in df.iterrows():
            fk_table = row["外键关联表"]
            fk_field = row["外键字段"]

            if fk_table and fk_field:
                dependencies[row["字段名"]] = {"field": fk_field, "func": ""}
                dep_table = fk_table

        st.session_state.config[table_name]["primary_key"] = primary_key
        st.session_state.config[table_name]["dependencies"] = dependencies
        st.session_state.config[table_name]["dep_table"] = dep_table
        st.session_state.config[table_name]["dep_relation"] = dep_relation

    def save_all_to_file(self, filename="dependency.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(st.session_state.config, f, indent=4, ensure_ascii=False)
        return filename

    def display_card_for_field(self, row, table_name, index):
        field_name = row["字段名"]
        key_prefix = f"{table_name}_{field_name}"

        with st.expander(f"🧩 字段：{field_name} ({row['类型']})", expanded=False):
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

            # 是否主键
            is_primary_key = st.checkbox("是否主键", value=row["是否主键"], key=f"{key_prefix}_is_pk")

            return {
                "字段名": field_name,
                "类型": row["类型"],
                "外键关联表": fk_table,
                "外键字段": fk_field,
                "是否主键": is_primary_key
            }
        return None

    def render_main_page(self):

        col1, col2 = st.columns([4, 1], vertical_alignment="bottom")

        with col1:
            selected_table = st.selectbox("请选择要配置的表", self.tables, key="main_table_selector")

        with col2:
            if st.button("💾 保存所有表", use_container_width=True):
                file_path = self.save_all_to_file()
                # st.toast(f"✅ 所有配置已保存到 `{file_path}`", icon='📄')

        st.divider()

        if selected_table:
            df = self.get_initial_table_data(selected_table)

            updated_rows = []

            for i, row in df.iterrows():
                result = self.display_card_for_field(row, selected_table, i)
                if result:
                    updated_rows.append(result)

            if st.button("✅ 保存当前表配置", use_container_width=True):
                new_df = pd.DataFrame(updated_rows)
                self.save_configuration(selected_table, new_df)
                st.success("✅ 当前表配置已保存！")

            with st.expander("👀 查看当前表配置", expanded=False):
                print("查看当前表配置")
                print(st.session_state.config[selected_table])
                st.json(st.session_state.config[selected_table])

    def show_config_modal_after_import(self):
        modal = Modal("🔧 表结构配置器", key="config_table_modal", padding=20, max_width=1300)
        if st.button("⚙️ 表结构配置", key="config_trigger_btn"):
            modal.open()
        if modal.is_open():
            with modal.container():
                self.render_main_page()
