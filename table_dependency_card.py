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
    "gtin_fpc_dim_sample": [{"字段名": "fpc_id", "类型": "string"}, {"字段名": "gtin", "类型": "string"}],
    "gtin_price_advance_dim_sample": [{"字段名": "price_date", "类型": "date"}, {"字段名": "price", "类型": "float"}],
    "sales_channel_man_dim_sample": [{"字段名": "channel_id", "类型": "string"}, {"字段名": "channel_name", "类型": "string"}],
    "store_dim_sample": [{"字段名": "store_id", "类型": "string"}, {"字段名": "store_name", "类型": "string"}],
    "t90_sample": [{"字段名": "order_id", "类型": "string"}, {"字段名": "gtin", "类型": "string"},
                   {"字段名": "store_id", "类型": "string"}],
    "gtin_dim_sample": [
        {"字段名": "gtin_skey", "类型": "string"},
        {"字段名": "gtin_code", "类型": "string"},
        {"字段名": "barcode_type", "类型": "string"},
        {"字段名": "fpc_code", "类型": "string"},
        {"字段名": "product_source", "类型": "string"},
        {"字段名": "item_status", "类型": "string"},
        {"字段名": "status", "类型": "string"},
        {"字段名": "item_nature", "类型": "string"},
        {"字段名": "shipper_barcode", "类型": "string"},
        {"字段名": "item_barcode", "类型": "string"},
        {"字段名": "inner_barcode", "类型": "string"},
        {"字段名": "sell_barcode", "类型": "string"},
        {"字段名": "product_name_cn", "类型": "string"},
        {"字段名": "product_specification", "类型": "string"},
        {"字段名": "product_name_en", "类型": "string"},
        {"字段名": "sector", "类型": "string"},
        {"字段名": "sub_sector", "类型": "string"},
        {"字段名": "category", "类型": "string"},
        {"字段名": "brand", "类型": "string"},
        {"字段名": "sub_brand", "类型": "string"},
        {"字段名": "brand_segment", "类型": "string"},
        {"字段名": "brand_form", "类型": "string"},
        {"字段名": "brand_element", "类型": "string"},
        {"字段名": "category_code", "类型": "string"},
        {"字段名": "category_en", "类型": "string"},
        {"字段名": "full_category_en", "类型": "string"},
        {"字段名": "category_cn", "类型": "string"},
        {"字段名": "brand_code", "类型": "string"},
        {"字段名": "brand_en", "类型": "string"},
        {"字段名": "full_brand_en", "类型": "string"},
        {"字段名": "brand_cn", "类型": "string"},
        {"字段名": "sub_brand_code", "类型": "string"},
        {"字段名": "sub_brand_en", "类型": "string"},
        {"字段名": "sub_brand_cn", "类型": "string"},
        {"字段名": "product_form_code", "类型": "string"},
        {"字段名": "product_form_en", "类型": "string"},
        {"字段名": "product_form_cn", "类型": "string"},
        {"字段名": "brand_product_form_en", "类型": "string"},
        {"字段名": "brand_product_form_cn", "类型": "string"},
        {"字段名": "variant_code", "类型": "string"},
        {"字段名": "variant_en", "类型": "string"},
        {"字段名": "full_variant_en", "类型": "string"},
        {"字段名": "variant_cn", "类型": "string"},
        {"字段名": "full_variant_cn", "类型": "string"},
        {"字段名": "price_tier", "类型": "string"},
        {"字段名": "new_form", "类型": "string"},
        {"字段名": "flag", "类型": "string"},
        {"字段名": "funded_product_form", "类型": "string"},
        {"字段名": "sap_size_total", "类型": "string"},
        {"字段名": "sap_size_main_product", "类型": "string"},
        {"字段名": "sap_size_combined", "类型": "string"},
        {"字段名": "cn_size_total", "类型": "string"},
        {"字段名": "cn_size_main_product", "类型": "string"},
        {"字段名": "cn_size_combined", "类型": "string"},
        {"字段名": "cn_size_unit", "类型": "string"},
        {"字段名": "diaper_size", "类型": "string"},
        {"字段名": "size_segment", "类型": "string"},
        {"字段名": "size_segment_cn", "类型": "string"},
        {"字段名": "case_cnt", "类型": "string"},
        {"字段名": "su_factor", "类型": "string"},
        {"字段名": "net_weight", "类型": "string"},
        {"字段名": "gross_weight", "类型": "string"},
        {"字段名": "weight_unit", "类型": "string"},
        {"字段名": "case_volume", "类型": "string"},
        {"字段名": "volume_unit", "类型": "string"},
        {"字段名": "components", "类型": "string"},
        {"字段名": "sos_date", "类型": "string"},
        {"字段名": "length", "类型": "string"},
        {"字段名": "width", "类型": "string"},
        {"字段名": "height", "类型": "string"},
        {"字段名": "case_length", "类型": "string"},
        {"字段名": "case_width", "类型": "string"},
        {"字段名": "case_height", "类型": "string"},
        {"字段名": "dim_unit", "类型": "string"},
        {"字段名": "ni_flag", "类型": "string"},
        {"字段名": "ni_project_name", "类型": "string"},
        {"字段名": "ni_sos_date", "类型": "string"},
        {"字段名": "local_hierarchy_flag", "类型": "string"},
        {"字段名": "last_shipment_date", "类型": "string"},
        {"字段名": "inactive_date", "类型": "string"},
        {"字段名": "bu_attr", "类型": "string"},
        {"字段名": "update_type", "类型": "string"},
        {"字段名": "import_item_type_code", "类型": "string"},
        {"字段名": "import_item_type_name", "类型": "string"},
        {"字段名": "category_2_code", "类型": "string"},
        {"字段名": "category_2_name_en", "类型": "string"},
        {"字段名": "full_category_2_name_en", "类型": "string"},
        {"字段名": "category_2_name_cn", "类型": "string"}
    ]
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

    col1, col2, col3 = st.columns([4, 1, 1])
    with col1:
        selected_table = st.selectbox("请选择要配置的表", tables, key="main_table_selector")
    with col2:
        if st.button("💾 保存当前表", use_container_width=True):
            if selected_table in st.session_state.config:
                st.toast(f"✅ `{selected_table}` 配置已保存！", icon='📄')
            else:
                st.warning("⚠️ 当前表没有可编辑的配置！")
    with col3:
        if st.button("📁 保存全部", use_container_width=True):
            st.toast(f"✅ 所有配置已保存！", icon='📄')

    st.markdown("---")

    if selected_table:
        df = get_initial_table_data(selected_table)

        # 搜索框
        search_term = st.text_input("🔍 搜索字段名", "")
        if search_term:
            df = df[df["字段名"].str.contains(search_term, case=False, na=False)]

        # Tabs 切换
        tab1, tab2 = st.tabs(["🛠️ 字段管理", "👀 当前配置"])

        with tab1:
            st.markdown('<div>', unsafe_allow_html=True)
            updated_rows = []
            for i, row in df.iterrows():
                result = display_card_for_field(row, selected_table, i)
                if result:
                    updated_rows.append(result)

            if st.button("✅ 保存当前表配置", use_container_width=True, type="primary"):
                new_df = pd.DataFrame(updated_rows)
                save_configuration(selected_table, new_df)
                st.success("✅ 当前表配置已保存！")

        with tab2:
            st.json(st.session_state.config.get(selected_table, []))


# ========== 运行入口 ===========
if __name__ == "__main__":
    render_main_page(tables)
