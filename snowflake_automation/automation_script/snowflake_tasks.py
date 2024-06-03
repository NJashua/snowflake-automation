import snowflake.connector
from config import Config

def get_snowflake_connection():
    return snowflake.connector.connect(**Config.as_dict())

def copy_schema(source_schema, destination_schema):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA {destination_schema} CLONE {source_schema}")
    cur.close()
    conn.close()

def create_warehouse(warehouse_name, warehouse_size):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"""
        CREATE WAREHOUSE {warehouse_name}
        WITH
        WAREHOUSE_SIZE = '{warehouse_size}'
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE
    """)
    cur.close()
    conn.close()

def create_resource_monitor(monitor_name, credit_quota):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE RESOURCE MONITOR {monitor_name}
            SET CREDIT_QUOTA = {credit_quota}
            TRIGGERS ON 90 PERCENT DO SUSPEND
        """)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


def create_role(role_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE ROLE {role_name}")
    cur.close()
    conn.close()

def grant_select_on_table(role_name, database_name, schema_name, table_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        # Grant SELECT privilege on the specified table
        cur.execute(f"GRANT SELECT ON TABLE {database_name}.{schema_name}.{table_name} TO ROLE {role_name}")
        print(f"Granted SELECT on {database_name}.{schema_name}.{table_name} to role {role_name}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


def get_query_performance():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE EXECUTION_STATUS = 'SUCCESS'
        ORDER BY START_TIME DESC
        LIMIT 10
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

# creating user module!!!

def create_user(user_name, user_email, user_password):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE USER {user_name}
            PASSWORD = '{user_password}'
            LOGIN_NAME = '{user_name}'
            EMAIL = '{user_email}'
        """)

        print("User created successfully")
    finally:
        cur.close()
        conn.close()
