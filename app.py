import streamlit as st
from snowflake_automation.automation_script import snowflake_tasks
import time
import pandas as pd

def admin_footer():
    if st.sidebar.radio("JESUS", ["Admin", "User"]) == "Admin":
        st.sidebar.title("Admin Actions, kamal hassan")
        st.title("DATA LAKEAR")

        st.header("Copy Realm Schema")
        source_schema = st.text_input('Source Schema')
        destination_schema = st.text_input('Destination Schema')
        destination_database = st.text_input('Database Name')

        if st.button('Copy Schema'):
            with st.spinner("Please wait..."):
                time.sleep(3)
                success = snowflake_tasks.copy_schema(source_schema, destination_schema, destination_database)
                if success:
                    st.success("Schema copied successfully... :)")
                else:
                    st.error("An error occurred while copying the schema. Please check the logs for more details.")

        st.header("Copy Schema DB to DB")
        source_schema = st.text_input('Source Schema', key='source_schema')
        destination_schema = st.text_input('Destination Schema', key='destination_schema')
        source_database = st.text_input('Source Database Name', key='source_database')
        destination_database = st.text_input('Destination Database Name', key='destination_database')

        if st.button("Copy DB-DB"):
            with st.spinner("Please Wait..."):
                time.sleep(3)  # Simulate a time-consuming operation
                snowflake_tasks.copy_schema_database(source_schema, destination_schema, source_database, destination_database)
                st.success("Schema copied to database successfully.")


        # Create Warehouse
        st.header("Create Warehouse")
        warehouse_name = st.text_input("Warehouse Name")
        warehouse_size = st.selectbox('Warehouse Size', ['X-SMALL', 'SMALL', 'MEDIUM', 'LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE', '5X-LARGE', '6X-LARGE'])
        if st.button('Create Warehouse'):
            with st.spinner("please wait"):
                time.sleep(3)
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

        # Grant Privileges!!!
        st.header("Grant Privileges")
        #st.subheader("Grant SELECT Privilege on a Table")

        user_name_input = st.text_input("User Name", key="user_name_input")
        role_name_select = st.text_input("Role Name", key='role_name_select')
        database_name = st.text_input("Database Name", key='database_name')
        schema_name = st.text_input("Schema Name", key='schema_name')
        table_name = st.text_input("Table Name", key='table_name')

        if st.button("Grant SELECT Privilege"):
            try:
                snowflake_tasks.grant_select_on_table(user_name_input, role_name_select, database_name, schema_name, table_name)
                st.success("Granted SELECT privilege on table successfully...:)")
            except Exception as e:
                st.error(f"An error occurred: {e}")

        # Query Performance!!!
        st.header("Query Performance")
        if st.button('Show Recent Queries'):
            results = snowflake_tasks.get_query_performance()
            st.write(results)

        # Creating user!!!
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

        st.header('Drop User')
        user_names = st.text_input("User Name", key="user_names")
        if st.button("Drop User"):
            try:
                snowflake_tasks.drop_user(user_names)
                st.success("User removed successfully")
            except Exception as e:
                st.error(f"An error occurred: {e}")
        
        st.title("Data Lake Account Dashboard")

        st.header("Available Users")
        try:
            users_data = snowflake_tasks.get_users()
            if users_data:
                st.table(users_data)
                st.success("Users data retrieved successfully.")
            else:
                st.write("No users found.")
        except Exception as e:
            st.error(f"Error fetching users data: {e}")

        # Fetching data!!!
        total_credits_used, daily_credits = snowflake_tasks.fetch_credits_data()

        # Converting fetched data into DataFrame!!!
        daily_credits_df = pd.DataFrame(daily_credits, columns=['usage_date', 'daily_credits_used'])
        allocated_credits = 400
        remaining_credits = allocated_credits - float(total_credits_used) if total_credits_used else 0  # Convert to float
        # Display metrics!!!
        st.metric(label="Total Allocated Credits", value=allocated_credits)
        st.metric(label="Total Credits Used", value=float(total_credits_used) if total_credits_used else 0)  # Convert to float
        st.metric(label="Remaining Credits", value=remaining_credits)

        # Process daily credits data!!!
        st.subheader("Daily Credits Usage")
        if not daily_credits_df.empty:
            # Generate plot for daily credits usage!!!
            fig = snowflake_tasks.plot_credits_usage(daily_credits_df, daily_credits_df['daily_credits_used'])
            st.pyplot(fig)
        else:
            st.write("No data available for daily credits usage.")

        # Display daily credits data in a table!!!
        st.subheader('Daily Credits Usage Data')
        st.dataframe(daily_credits_df)

admin_footer()
