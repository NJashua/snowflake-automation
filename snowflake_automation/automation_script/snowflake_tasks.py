# import snowflake.connector
# from config import Config
# import logging

# def get_snowflake_connection():
#     return snowflake.connector.connect(**Config.as_dict())

# def copy_schema(source_schema, destination_schema, source_database=None, destination_database=None):
#     conn = None
#     try:
#         conn = get_snowflake_connection()
#         cur = conn.cursor()

#         logging.info(f"Attempting to clone schema '{source_schema}' to '{destination_schema}' in database '{destination_database}'")

#         # Construct the SQL statement based on whether source and destination databases are provided
#         # if source_database and destination_database:
#         #     sql_statement = f"""
#         #         CREATE SCHEMA IF NOT EXISTS "{destination_database.upper()}"."{destination_schema.upper()}"
#         #         CLONE "{source_database.upper()}"."{source_schema.upper()}"
#         #     """
#         # else:
#         sql_statement = f"CREATE SCHEMA IF NOT EXISTS {destination_schema.upper()} CLONE {source_schema.upper()}"

#         cur.execute(sql_statement)
#         cur.close()

#         logging.info(f"Schema '{source_schema}' successfully cloned to '{destination_schema}' in database '{destination_database}'")
#         return True

#     except Exception as e:
#         logging.error(f"Error occurred while copying schema: {e}")
#         return False

#     finally:
#         if conn:
#             conn.close()

# def copy_schema_database(source_schema, destination_schema, source_database=None, destination_database=None):
#     conn = None
#     try:
#         conn = get_snowflake_connection()
#         cur = conn.cursor()
#         logging.info(f"Attempting to clone schema '{source_schema}' to '{destination_schema}' in database '{destination_database}'")
        
#         if source_database is not None and destination_database is not None:
#             # Correcting SQL statement format
#             sql_statement = f"""CREATE SCHEMA IF NOT EXISTS "{destination_database.upper()}"."{destination_schema.upper()}" CLONE "{source_database.upper()}"."{source_schema}" """
#             cur.execute(sql_statement)
#             logging.info("Schema cloned successfully.")
#         else:
#             logging.error("Source and/or destination database names are missing or empty.")


#     except Exception as e:
#         logging.error(f"An error occurred: {str(e)}")
#     finally:
#         if conn:
#             conn.close()


# def grant_ownership_on_database(database_name, user_name):
#     conn = None
#     try:
#         conn = get_snowflake_connection()
#         cur = conn.cursor()
#         logging.info(f"Granting ownership on database '{database_name}' to user '{user_name}'")
        
#         if database_name:
#             sql_statement = f"GRANT OWNERSHIP ON DATABASE {database_name} TO {user_name}"
#             cur.execute(sql_statement)
#             logging.info("Ownership granted successfully.")
#         else:
#             logging.error("Database name is missing or empty.")

#     except Exception as e:
#         logging.error(f"An error occurred: {str(e)}")
#     finally:
#         if conn:
#             conn.close()



# def create_warehouse(warehouse_name, warehouse_size):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     cur.execute(f"""
#         CREATE WAREHOUSE {warehouse_name}
#         WITH
#         WAREHOUSE_SIZE = '{warehouse_size}'
#         AUTO_SUSPEND = 300
#         AUTO_RESUME = TRUE
#         INITIALLY_SUSPENDED = TRUE
#     """)
#     cur.close()
#     conn.close()

# def create_resource_monitor(monitor_name, credit_quota):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     try:
#         cur.execute(f"""
#             CREATE RESOURCE MONITOR {monitor_name}
#             SET CREDIT_QUOTA = {credit_quota}
#             TRIGGERS ON 90 PERCENT DO SUSPEND
#         """)
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         cur.close()
#         conn.close()


# def create_role(role_name):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     cur.execute(f"CREATE ROLE {role_name}")
#     cur.close()
#     conn.close()

# def grant_select_on_table(role_name, database_name, schema_name, table_name):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     try:
#         # Grant SELECT privilege on the specified table
#         cur.execute(f"GRANT SELECT ON TABLE {database_name}.{schema_name}.{table_name} TO ROLE {role_name}")
#         print(f"Granted SELECT on {database_name}.{schema_name}.{table_name} to role {role_name}")
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         cur.close()
#         conn.close()


# def get_query_performance():
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     cur.execute("""
#         SELECT *
#         FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
#         WHERE EXECUTION_STATUS = 'SUCCESS'
#         ORDER BY START_TIME DESC
#         LIMIT 10
#     """)
#     results = cur.fetchall()
#     cur.close()
#     conn.close()
#     return results

# # creating user module!!!

# def create_user(user_name, user_email, user_password):
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
#     try:
#         cur.execute(f"""
#             CREATE USER {user_name}
#             PASSWORD = '{user_password}'
#             LOGIN_NAME = '{user_name}'
#             EMAIL = '{user_email}'
#         """)

#         print("User created successfully")
#     finally:
#         cur.close()
#         conn.close()

import snowflake.connector
from config import Config
import logging

# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(**Config.as_dict())

# Function to copy schema within a database
def copy_schema(source_schema, destination_schema, destination_database=None):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        logging.info(f"Attempting to clone schema '{source_schema}' to '{destination_schema}' in database '{destination_database}'")
        sql_statement = f"CREATE SCHEMA IF NOT EXISTS {destination_schema.upper()} CLONE {source_schema.upper()}"

        cur.execute(sql_statement)
        cur.close()

        logging.info(f"Schema '{source_schema}' successfully cloned to '{destination_schema}' in database '{destination_database}'")
        return True

    except Exception as e:
        logging.error(f"Error occurred while copying schema: {e}")
        return False

    finally:
        if conn:
            conn.close()

# Function to copy schema between databases
def copy_schema_database(source_schema, destination_schema, source_database=None, destination_database=None):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        logging.info(f"Attempting to clone schema '{source_schema}' to '{destination_schema}' in database '{destination_database}'")
        
        if source_database is not None and destination_database is not None:
            # Correcting SQL statement format
            sql_statement = f"""CREATE SCHEMA IF NOT EXISTS "{destination_database.upper()}"."{destination_schema.upper()}" CLONE "{source_database.upper()}"."{source_schema}" """
            cur.execute(sql_statement)
            logging.info("Schema cloned successfully...:)")
        else:
            logging.error("Source and/or destination database names are missing or empty.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()

# Function to grant ownership of a database
def grant_ownership_on_database(database_name, user_name):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        logging.info(f"Granting ownership on database '{database_name}' to user '{user_name}'")
        
        if database_name:
            sql_statement = f"GRANT OWNERSHIP ON DATABASE {database_name} TO {user_name}"
            cur.execute(sql_statement)
            logging.info("Ownership granted successfully.")
        else:
            logging.error("Database name is missing or empty.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()

# Function to create a warehouse
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

# Function to create a resource monitor
def create_resource_monitor(monitor_name, credit_quota):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE RESOURCE MONITOR {monitor_name}
            SET CREDIT_QUOTA = {credit_quota}
            NOTIFY_USERS = (NITHIN, 'Nithin Nithin', 'Keerthan')
            TRIGGERS ON 90 PERCENT DO SUSPEND
        """)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

# Function to create a role
def create_role(role_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE ROLE {role_name}")
    cur.close()
    conn.close()

# Function to grant SELECT privilege on a table
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

# Function to retrieve recent query performance
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

# Function to create a user
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