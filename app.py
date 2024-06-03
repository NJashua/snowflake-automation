import streamlit as st
from snowflake_automation.automation_script import snowflake_tasks

st.title("Snowflake Automation")

# Copy Schema
st.header("Copy Realm Schema")
source_schema = st.text_input('Source Schema')
destination_schema = st.text_input('Destination Schema')
if st.button('Copy Schema'):
    snowflake_tasks.copy_schema(source_schema, destination_schema)
    st.success("Schema copied successfully...:)")

# Create Warehouse
st.header("Create Warehouse")
warehouse_name = st.text_input("Warehouse Name")
warehouse_size = st.selectbox('Warehouse Size', ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE'])
if st.button('Create Warehouse'):
    snowflake_tasks.create_warehouse(warehouse_name, warehouse_size)
    st.success("Warehouse created successfully...:)")

# Create Resource Monitor
st.header("Create Resource Monitor")
monitor_name = st.text_input('Monitor Name')
credit_quota = st.number_input('Credit Quota', min_value=0)
if st.button('Create Monitor'):
    snowflake_tasks.create_resource_monitor(monitor_name, credit_quota)
    st.success("Resource monitor created successfully..:)")


# Create Role
st.header("Create Role")
role_name = st.text_input("Role Name", key='role_name')
if st.button('Create Role'):
    snowflake_tasks.create_role(role_name)
    st.success("Role created successfully..:)")

# Grant Privileges
st.header("Grant Privileges")
st.subheader("Grant SELECT Privilege on a Table")

role_name_select = st.text_input("Role Name", key='role_name_select')
database_name = st.text_input("Database Name", key='database_name')
schema_name = st.text_input("Schema Name", key='schema_name')
table_name = st.text_input("Table Name", key='table_name')
if st.button("Grant SELECT Privilege"):
    snowflake_tasks.grant_select_on_table(role_name_select, database_name, schema_name, table_name)
    st.success("Granted SELECT privilege on table successfully...:)")


# Query Performance
st.header("Query Performance")
if st.button('Show Recent Queries'):
    results = snowflake_tasks.get_query_performance()
    st.write(results)

# creating user!!!
st.header("Create User")
user_name = st.text_input("User Name", key='user_name')
user_email = st.text_input("User Email", key='user_email')
user_password = st.text_input("User Password", type="password", key='user_password')
user_confirm_password = st.text_input("Confirm Password", type="password", key='user_confirm_password')
if st.button("Create User"):
    if user_password == user_confirm_password:
        snowflake_tasks.create_user(user_name, user_email, user_password)
        st.success("User created successfully..:)")
    else:
        st.error("Passwords do not match. Please try again.")