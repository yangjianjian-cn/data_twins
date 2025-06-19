import json
import pandas as pd
import streamlit as st

# 示例表列表
tables = [
    "gtin_dim_sample",
    "gtin_fpc_dim_sample",
    "gtin_price_advance_dim_sample",
    "sales_channel_man_dim_sample",
    "store_dim_sample",
    "t90_sample"
]

# 模拟每个表的字段信息
field_info = {
    "gtin_dim_sample": [{"字段名": "gtin", "类型": "string"}, {"字段名": "product_name", "类型": "string"}],
    "gtin_fpc_dim_sample": [{"字段名": "fpc_id", "类型": "string"}, {"字段名": "gtin", "类型": "string"}],
    "gtin_price_advance_dim_sample": [{"字段名": "price_date", "类型": "date"}, {"字段名": "price", "类型": "float"}],
    "sales_channel_man_dim_sample": [{"字段名": "channel_id", "类型": "string"}, {"字段名": "channel_name", "类型": "string"}],
    "store_dim_sample": [{"字段名": "store_id", "类型": "string"}, {"字段名": "store_name", "类型": "string"}],
    "t90_sample": [{"字段名": "order_id", "类型": "string"}, {"字段名": "gtin", "类型": "string"},
                   {"字段名": "store_id", "类型": "string"}]
}

# 初始化默认配置
default_config = {
    table: {
        "primary_key": [],
        "foreign_keys": []
    } for table in tables
}

# 使用 session_state 保存状态
if 'config' not in st.session_state:
    st.session_state.config = default_config.copy()

if 'editor_refresh' not in st.session_state:
    st.session_state.editor_refresh = 0


# ================== 配置初始化相关函数 ==================
def get_initial_table_data(table_name):
    df_fields = pd.DataFrame(field_info[table_name])
    config = st.session_state.config[table_name]

    data = []
    for _, row in df_fields.iterrows():
        field_name = row["字段名"]
        fk_info = next((item for item in config["foreign_keys"] if item["字段名"] == field_name), {})
        data.append({
            "字段名": field_name,
            "类型": row["类型"],
            "外键关联表": fk_info.get("外键关联表", ""),
            "外键字段": fk_info.get("外键字段", ""),
            "是否主键": field_name in config["primary_key"]
        })
    return pd.DataFrame(data)


def build_column_config(df, table_name):
    # 动态生成“外键字段”的每行下拉选项
    fk_field_options = {}
    for i, row in df.iterrows():
        fk_table = row["外键关联表"]
        if fk_table and fk_table in field_info:
            options = [f["字段名"] for f in field_info[fk_table]]
        else:
            options = []
        fk_field_options[row["字段名"]] = [""] + options  # 加上空值作为默认

    column_config = {
        "字段名": st.column_config.TextColumn("字段名", disabled=True),
        "类型": st.column_config.TextColumn("类型", disabled=True),
        "外键关联表": st.column_config.SelectboxColumn("外键关联表", options=[""] + tables),
        "外键字段": st.column_config.SelectboxColumn(
            "外键字段",
            options=fk_field_options,
            disabled=False
        ),
        "是否主键": st.column_config.CheckboxColumn("是否主键")
    }

    return column_config


def save_configuration(table_name, edited_df):
    primary_key = edited_df[edited_df["是否主键"]]["字段名"].tolist()

    foreign_keys = []
    for _, row in edited_df.iterrows():
        fk_table = row["外键关联表"]
        fk_field = row["外键字段"]

        if fk_table and fk_field:
            foreign_keys.append({
                "字段名": row["字段名"],
                "外键关联表": fk_table,
                "外键字段": fk_field
            })

    st.session_state.config[table_name]["primary_key"] = primary_key
    st.session_state.config[table_name]["foreign_keys"] = foreign_keys


def save_all_to_file(filename="dependency.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(st.session_state.config, f, indent=4, ensure_ascii=False)
    return filename


# ================== 页面组件 ==================
def display_and_configure_table(table_name):
    # 获取当前表的数据
    df = get_initial_table_data(table_name)

    # 构建列配置
    column_config = build_column_config(df, table_name)

    # 使用唯一的 key 确保每次配置变化都会刷新编辑器
    editor_key = f"{table_name}_editor_{st.session_state.editor_refresh}"

    edited_df = st.data_editor(
        df,
        column_config=column_config,
        hide_index=True,
        num_rows="fixed",
        use_container_width=True,
        key=editor_key
    )

    submit = st.button("✅ 保存当前表配置", key=f"{table_name}_submit")

    if submit:
        save_configuration(table_name, edited_df)
        st.session_state.editor_refresh += 1
        st.rerun()

    with st.expander("👀 查看当前表配置", expanded=False):
        st.json(st.session_state.config[table_name])


# ================== 主程序入口 ==================
def render_main_page(tables):
    st.title("📦 表结构配置工具")

    col1, col2 = st.columns([3, 1], vertical_alignment="bottom")

    with col1:
        selected_table = st.selectbox("请选择要配置的表", tables, key="main_table_selector")

    with col2:
        if st.button("💾 保存所有配置", use_container_width=True):
            file_path = save_all_to_file()
            st.toast(f"✅ 所有配置已保存到 `{file_path}`", icon='📄')

    if selected_table:
        display_and_configure_table(selected_table)


# ========== 运行入口 ===========
if __name__ == "__main__":
    render_main_page(tables)
