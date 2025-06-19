import json
import streamlit as st
import pandas as pd

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

if 'config' not in st.session_state:
    st.session_state.config = default_config.copy()


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


def save_configuration(table_name, df):
    primary_key = df[df["是否主键"]]["字段名"].tolist()

    foreign_keys = []
    for _, row in df.iterrows():
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


# ================== 卡片式界面组件 ==================
def display_card_for_field(row, table_name, index):
    field_name = row["字段名"]
    key_prefix = f"{table_name}_{field_name}"

    with st.expander(f"🧩 字段：{field_name} ({row['类型']})", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            # 外键关联表
            fk_table = st.selectbox(
                "外键关联表",
                options=[""] + tables,
                index=0 if pd.isna(row["外键关联表"]) else tables.index(row["外键关联表"]) + 1 if row[
                                                                                            "外键关联表"] in tables else 0,
                key=f"{key_prefix}_fk_table"
            )

        with col2:
            # 动态生成外键字段下拉
            fk_field_options = [""]

            if fk_table and fk_table in field_info:
                fk_field_options += [f["字段名"] for f in field_info[fk_table]]

            fk_field = st.selectbox(
                "外键字段",
                options=fk_field_options,
                index=0 if pd.isna(row["外键字段"]) or row["外键字段"] not in fk_field_options else fk_field_options.index(
                    row["外键字段"]),
                key=f"{key_prefix}_fk_field"
            )

        # 是否主键
        is_primary_key = st.checkbox("是否主键", value=row["是否主键"], key=f"{key_prefix}_is_pk")

        # 返回当前字段更新后的数据
        return {
            "字段名": field_name,
            "类型": row["类型"],
            "外键关联表": fk_table,
            "外键字段": fk_field,
            "是否主键": is_primary_key}
    return None


# ================== 主程序入口 ==================
def render_main_page(tables):
    st.title("🧾 表结构配置工具")

    # 将下拉选择和保存按钮并排显示
    col1, col2 = st.columns([4, 1],vertical_alignment="bottom")

    with col1:
        selected_table = st.selectbox("请选择要配置的表", tables, key="main_table_selector")

    with col2:
        if st.button("💾 保存所有表", use_container_width=True):
            file_path = save_all_to_file()
            st.toast(f"✅ 所有配置已保存到 `{file_path}`", icon='📄')

    st.divider()

    if selected_table:
        df = get_initial_table_data(selected_table)

        updated_rows = []

        for i, row in df.iterrows():
            result = display_card_for_field(row, selected_table, i)
            if result:
                updated_rows.append(result)

        if st.button("✅ 保存当前表配置", use_container_width=True):
            new_df = pd.DataFrame(updated_rows)
            save_configuration(selected_table, new_df)
            st.success("✅ 当前表配置已保存！")

        with st.expander("👀 查看当前表配置", expanded=False):
            st.json(st.session_state.config[selected_table])


# ========== 运行入口 ===========
if __name__ == "__main__":
    render_main_page(tables)
