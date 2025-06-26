import io
import json
import os
import tempfile
from contextlib import redirect_stdout

import pandas as pd
import streamlit as st
import yaml
from streamlit_ace import st_ace

from gen_data_by_stats import gen_data_by_stats
from get_db_statistic import get_db_statistic
from save_data_to_db import save_data_to_db
from tools.ParquetExporter import ParquetExporter
from tools.TableDependence import TableConfigurator
from tools.import_excel_to_postgres import excel_to_db

st.set_page_config(layout="wide", page_title="🚀 数据孪生应用")

# 加载自定义CSS
with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.title("🚀 数据孪生应用")


# Function to load configuration
def load_config(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# Function to save configuration to a temporary file
def save_temp_config(config):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as temp_file:
        yaml.dump(config, temp_file, allow_unicode=True)
        return temp_file.name


# Function to convert specified columns to DataFrame
def specified_columns_to_df(specified_columns):
    data = []
    for table, columns in specified_columns.items():
        for column in columns:
            for field_name, field_type in column.items():
                data.append({"表名": table, "字段名": field_name, "字段类型": field_type})
    return pd.DataFrame(data)


# Function to convert DataFrame back to specified columns format
def df_to_specified_columns(df):
    specified_columns = {}
    for _, row in df.iterrows():
        table = row['表名']
        if table not in specified_columns:
            specified_columns[table] = []
        specified_columns[table].append({row['字段名']: row['字段类型']})
    return specified_columns


# Sidebar
st.sidebar.header("参数设置")

# List configuration files
config_files = [f for f in os.listdir() if f.endswith('.yaml')]
selected_config = st.sidebar.selectbox("选择配置文件", config_files, key="config_select")

# Add the "删除现有表" checkbox to the sidebar
drop_existing = st.sidebar.checkbox("删除现有表", value=True)

# Add row_num input to the sidebar
row_num = st.sidebar.number_input("最小生成行数", min_value=0, value=None, step=1, help="每张表生成的最小行数,留空则使用默认逻辑")

if selected_config:
    config = load_config(selected_config)

    # Display and edit source database connection info
    st.sidebar.subheader("源数据库")
    source_db = config.get('source_database', {})
    source_db['host'] = st.sidebar.text_input("源数据库主机", value=source_db.get('host', ''), key="source_host")
    source_db['port'] = st.sidebar.text_input("源数据库端口", value=str(source_db.get('port', '')), key="source_port")
    source_db['name'] = st.sidebar.text_input("源数据库名称", value=source_db.get('name', ''), key="source_name")
    source_db['user'] = st.sidebar.text_input("源数据库用户名", value=source_db.get('user', ''), key="source_user")
    source_db['password'] = st.sidebar.text_input("源数据库密码", value=source_db.get('password', ''), type="password",
                                                  key="source_password")

    # Display and edit target database connection info
    st.sidebar.subheader("目标数据库")
    target_db = config.get('target_database', {})
    target_db['host'] = st.sidebar.text_input("目标数据库主机", value=target_db.get('host', ''), key="target_host")
    target_db['port'] = st.sidebar.text_input("目标数据库端口", value=str(target_db.get('port', '')), key="target_port")
    target_db['name'] = st.sidebar.text_input("目标数据库名称", value=target_db.get('name', ''), key="target_name")
    target_db['user'] = st.sidebar.text_input("目标数据库用户名", value=target_db.get('user', ''), key="target_user")
    target_db['password'] = st.sidebar.text_input("目标数据库密码", value=target_db.get('password', ''), type="password",
                                                  key="target_password")

    # Update config with potentially modified values
    config['source_database'] = source_db
    config['target_database'] = target_db

    # Main content
    st.header("配置信息")
    data_import_successful = False
    uploaded_file = st.file_uploader("请选择源文件", type=["csv", "xlsx"])
    if uploaded_file is not None:
        # 读取 Excel 文件中的所有 sheet 名称
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = xls.sheet_names
        if len(sheet_names) < 1:
            st.error("Excel 文件至少需要一个 sheet 来定义字段类型！")
        else:
            st.success(f"检测到 {len(sheet_names)} 个 sheet：{', '.join(sheet_names)}")
            data_import_successful, table_info_list = excel_to_db(uploaded_file, sheet_names, config['source_database'])
    if data_import_successful:
        table_names = [table_info['table_name'] for table_info in table_info_list]
        fields = {
            table["table_name"]: [{"字段名": col, "类型": typ} for col, typ in table["columns"]]
            for table in table_info_list
        }
        configurator = TableConfigurator(tables=table_names, field_info=fields)
        configurator.show_config_modal_after_import(source_db)

    # Display and edit code tables
    st.subheader("代码表")
    codetables = st.text_area("代码表配置", value=yaml.dump(config.get('codetables', []), allow_unicode=True), height=150,
                              key="codetables")

    # Display and edit specified columns
    st.subheader("指定列")
    df = specified_columns_to_df(config.get('specified_columns', {}))
    edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True, key="specified_columns")

    try:
        # Update config with potentially modified values
        config['codetables'] = yaml.safe_load(codetables)
        config['specified_columns'] = df_to_specified_columns(edited_df)

        # Buttons for actions
        col1, col2, col3, col4 = st.columns(4)

        if col1.button("生成统计信息"):
            # Save the current config to a temporary file
            temp_config_file = save_temp_config(config)
            # print("temp_config_file:%s", temp_config_file)

            try:
                # Call get_db_statistic with the temporary config file
                get_db_statistic(temp_config_file)

                # Check if db_stats.json was created
                if os.path.exists('db_stats.json'):
                    # Load statistics
                    with open('db_stats.json', 'r', encoding='utf-8') as f:
                        statistics = json.load(f)

                    st.success("统计信息已更新并保存到 db_stats.json")
                    st.subheader("统计信息")

                    # Convert statistics to a formatted JSON string
                    formatted_json = json.dumps(statistics, indent=2, ensure_ascii=False)

                    # Display JSON in Ace editor
                    edited_json = st_ace(
                        value=formatted_json,
                        language="json",
                        theme="chrome",
                        keybinding="vscode",
                        min_lines=20,
                        max_lines=None,
                        font_size=14,
                        key="ace_editor"
                    )

                    if st.button("更新统计信息"):
                        try:
                            # Parse the edited JSON
                            updated_statistics = json.loads(edited_json)

                            # Save the updated statistics back to db_stats.json
                            with open('db_stats.json', 'w', encoding='utf-8') as f:
                                json.dump(updated_statistics, f, ensure_ascii=False, indent=4)

                            st.success("统计信息已更新并保存到 db_stats.json")
                        except json.JSONDecodeError as e:
                            st.error(f"JSON 格式错误: {str(e)}")
                else:
                    st.error("未能生成 db_stats.json 文件。请检查 get_db_statistic 函数的执行情况。")
            except Exception as e:
                st.error(f"生成统计信息时发生错误: {str(e)}")
            finally:
                # Clean up the temporary file
                os.remove(temp_config_file)

        if col2.button("生成数据"):
            try:
                generated_data = gen_data_by_stats(stats_file='db_stats.json', num_records=row_num)

                # Save generated data to JSON file
                with open('generated_data.json', 'w', encoding='utf-8') as f:
                    json.dump(generated_data, f, ensure_ascii=False, indent=4)

                st.success("数据已生成并保存到 generated_data.json")

                # Display generated data
                st.subheader("生成的数据")
                st.json(generated_data)

            except Exception as e:
                st.error(f"生成数据时发生错误: {str(e)}")

        if col3.button("存入数据库"):
            try:
                # 创建一个 StringIO 对象来捕获 print 输出
                output = io.StringIO()
                with redirect_stdout(output):
                    result = save_data_to_db(
                        source_config=config['source_database'],
                        target_config=config['target_database'],
                        data_file='generated_data.json',
                        drop_existing_tables=drop_existing
                    )

                # 获取捕获的输出
                output_str = output.getvalue()

                st.success("数据已存入数据库")

                # 显示捕获的输出
                st.subheader("数据库写入过程")
                st.text(output_str)

                # 显示函数返回的结果（如果有的话）
                if result:
                    st.subheader("数据库写入结果")
                    st.text(yaml.dump(result, allow_unicode=True))

            except Exception as e:
                st.error(f"存入数据库时发生错误: {str(e)}")

        if col4.button("📊 导出为Parquet"):
            json_file_path = os.path.join(os.getcwd(), "generated_data.json")
            downloader = ParquetExporter(json_file_path=json_file_path)

            if downloader.load_json():
                with st.spinner("🔄 正在打包所有子表为 ZIP 文件..."):
                    downloader.download_zip_button(file_name="all_tables_exported.zip")
            else:
                st.warning("⚠️ 数据加载异常。")

    except Exception as e:
        st.error(f"发生错误: {str(e)}")
else:
    st.warning("请选择一个配置文件")
