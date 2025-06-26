import streamlit as st
import os
import json

# 模拟数据库表结构
tables = {
    "gtin_dim_sample": ["gtin_skey", "gtin_code", "barcode_type"],
    "shipper_barcode": ["shipper_barcode", "item_barcode", "inner_barcode"],
    "gtin_fpc_dim_sample": ["gtin_code", "fpc_code"]
}

# 外键配置文件路径
FK_JSON_PATH = "foreign_keys.json"

# 默认新外键模板
NEW_FK_TEMPLATE = {
    "name": "new_fk",
    "target_table": "",
    "columns": [{"source": "", "target": ""}],
    "on_delete": "NO ACTION",
    "on_update": "NO ACTION",
    "deferrable": False,
    "initially_deferred": False
}


# 从文件加载 foreign_keys
def load_foreign_keys():
    if os.path.exists(FK_JSON_PATH):
        with open(FK_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        # 初始化为空列表
        return {table: [] for table in tables}


# 保存 foreign_keys 到文件
def save_foreign_keys(data):
    with open(FK_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


# 加载外键数据
foreign_keys = load_foreign_keys()


# 左侧边栏：显示表名 + 外键 或 “新建外键”
def render_sidebar(tables, foreign_keys):
    st.sidebar.title("Tables")

    for table_name in tables:
        with st.sidebar.expander(table_name, expanded=False):
            fk_list = foreign_keys.get(table_name, [])
            if not fk_list:
                if st.button("➕ 新建外键", key=f"new_fk_{table_name}"):
                    st.session_state.selected_table = table_name
                    st.session_state.selected_fk = NEW_FK_TEMPLATE.copy()
            else:
                for fk in fk_list:
                    if st.button(f"🔗 {fk['name']}", key=f"fk_{table_name}_{fk['name']}"):
                        st.session_state.selected_table = table_name
                        st.session_state.selected_fk = fk
                # 即使有外键，也保留新建按钮
                if st.button("➕ 新建外键", key=f"new_fk_after_{table_name}"):
                    st.session_state.selected_table = table_name
                    st.session_state.selected_fk = NEW_FK_TEMPLATE.copy()


# 右侧面板：外键详细配置（支持新建和编辑）
def render_fk_details():
    if 'selected_fk' not in st.session_state or st.session_state.selected_fk is None:
        st.info("请在左侧选择一个表和外键以开始配置")
        return

    fk = st.session_state.selected_fk
    table_name = st.session_state.selected_table

    if fk["name"] == "new_fk":
        st.header("🆕 新建外键")
    else:
        st.header(f"🔧 配置外键: {fk['name']}")

    st.markdown(f"来源表: `{table_name}`")

    with st.form(key="fk_config_form"):
        col1, col2 = st.columns(2)
        with col1:
            target_table_options = [t for t in tables.keys() if t != table_name]  # 排除自己
            default_index = 0
            if fk["target_table"] in target_table_options:
                default_index = target_table_options.index(fk["target_table"])
            target_table = st.selectbox("目标表", options=target_table_options, index=default_index)
        with col2:
            name = st.text_input("外键名称", value=fk["name"])

        source_cols = tables[table_name]
        target_cols = tables[target_table]

        # 如果目标表发生变化，清空映射并设置重置标志
        if 'last_target_table' not in st.session_state:
            st.session_state.last_target_table = target_table
        if 'reset_target_cols_flag' not in st.session_state:
            st.session_state.reset_target_cols_flag = hash(target_table)

        if st.session_state.last_target_table != target_table:
            st.session_state.last_target_table = target_table
            st.session_state.reset_target_cols_flag = hash(target_table)  # 使用哈希作为唯一标志
            fk["columns"] = [{"source": "", "target": ""}]
            st.rerun()

        st.subheader("列映射")
        updated_mappings = []

        for i, mapping in enumerate(fk["columns"]):
            col1, col2 = st.columns(2)
            with col1:
                source_col = st.selectbox(
                    f"源列 {i + 1}",
                    options=[""] + source_cols,
                    index=[""] + source_cols.index(mapping["source"]) if mapping["source"] in source_cols else 0,
                    key=f"source_col_{i}"
                )
            with col2:
                target_col = st.selectbox(
                    f"目标列 {i + 1}",
                    options=[""] + tables[target_table],
                    index=[""] + tables[target_table].index(mapping["target"]) if mapping["target"] in tables[
                        target_table] else 0,
                    key=f"target_col_{i}_{st.session_state.reset_target_cols_flag}"  # 关键点：加入 flag 保证刷新
                )
            updated_mappings.append({"source": source_col, "target": target_col})

        # 添加更多列映射行
        if st.form_submit_button("➕ 添加列映射"):
            updated_mappings.append({"source": "", "target": ""})
            fk["columns"] = updated_mappings
            st.rerun()

        st.subheader("约束行为")
        col3, col4 = st.columns(2)
        with col3:
            on_delete = st.selectbox("On Delete", ["NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT"],
                                     index=["NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT"].index(fk["on_delete"]))
        with col4:
            on_update = st.selectbox("On Update", ["NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT"],
                                     index=["NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT"].index(fk["on_update"]))

        deferrable = st.checkbox("Deferrable", value=fk["deferrable"])
        initially_deferred = st.checkbox("Initially Deferred", value=fk["initially_deferred"])

        if st.form_submit_button("✅ 保存外键配置"):
            # 更新 fk 字段
            fk["name"] = name
            fk["target_table"] = target_table
            fk["columns"] = updated_mappings
            fk["on_delete"] = on_delete
            fk["on_update"] = on_update
            fk["deferrable"] = deferrable
            fk["initially_deferred"] = initially_deferred

            # 如果是新外键，则加入 foreign_keys 中
            if fk["name"] == "new_fk":
                fk["name"] = f"fk_{table_name}_to_{target_table}"
                if table_name not in foreign_keys:
                    foreign_keys[table_name] = []
                foreign_keys[table_name].append(fk)
            else:
                # 替换原有外键（如果编辑了已有外键）
                for i, existing_fk in enumerate(foreign_keys[table_name]):
                    if existing_fk["name"] == fk["name"]:
                        foreign_keys[table_name][i] = fk
                        break

            # 保存到文件
            save_foreign_keys(foreign_keys)
            st.success("✅ 外键配置已保存！")


# 主程序入口
def main():
    st.set_page_config(page_title="Database Foreign Key Editor", layout="wide")
    st.title("🧱 数据库外键配置工具")

    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = None
    if 'selected_fk' not in st.session_state:
        st.session_state.selected_fk = None
    if 'last_target_table' not in st.session_state:
        st.session_state.last_target_table = None

    render_sidebar(tables, foreign_keys)
    render_fk_details()


if __name__ == "__main__":
    main()
