import snowflake.connector
from config import Config
import logging
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

def get_snowflake_connection():
    return snowflake.connector.connect(**Config.as_dict())

# schema to schema within a database
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

# Function to copy schema between databases to database
def copy_schema_database(source_schema, destination_schema, source_database=None, destination_database=None):
    conn = None
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        logging.info(f"Attempting to clone schema '{source_schema}' to '{destination_schema}' in database '{destination_database}'")
        
        if source_database is not None and destination_database is not None:
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

# granting ownershipt to user for database
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

# warehouse creation function!!!
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

# resource creation function, deviding credits for particular uage
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

# creating roles!!!
def create_role(role_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE ROLE {role_name}")
    cur.close()
    conn.close()

# grant access to specified tables or etc
def grant_select_on_table(user_name_input, role_name, database_name, schema_name, table_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"GRANT ROLE {role_name} TO USER {user_name_input}")
        logging.info(f"Granted role {role_name} to user {user_name_input}")
    
        cur.execute(f"GRANT SELECT ON TABLE {database_name}.{schema_name}.{table_name} TO ROLE {role_name}")
        logging.info(f"Granted SELECT on {database_name}.{schema_name}.{table_name} to role {role_name}")
        
        print(f"Granted SELECT on {database_name}.{schema_name}.{table_name} to role {role_name}")
    except Exception as e:
        print(f"Error: {e}")
        logging.error(f"Error granting SELECT privilege: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

# getting recent commands history in snowflake!!!
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

# creating user!!!
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

        logging.info("User created successfully..:)")
    finally:
        cur.close()
        conn.close()

# drop user!!!
def drop_user(user_name):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"DROP USER {user_name}")
        logging.info("User deleted successfully..:)")
    except Exception as e:
        logging.error(f"Error deleting user: {e}")
        raise e
    finally:
        cur.close()
        conn.close()


# credits data fetching!!!
def fetch_credits_data():
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        # Fetch total credits used in the last month
        cur.execute("""
            SELECT 
                SUM(credits_used) AS total_credits_used 
            FROM 
                SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE 
                start_time >= DATEADD('month', -1, CURRENT_DATE());

        """)
        total_credits_used = cur.fetchone()[0]

        # Fetch daily usage credits in a month
        cur.execute("""
            SELECT 
                start_time::date AS usage_date, 
                SUM(credits_used) AS daily_credits_used
            FROM 
                SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE 
                start_time >= DATEADD('month', -1, CURRENT_DATE())
            GROUP BY 
                start_time::date
            ORDER BY 
                usage_date
        """)
        daily_credits = cur.fetchall()

    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        total_credits_used = None
        daily_credits = []

    finally:
        cur.close()
        conn.close()

    return total_credits_used, daily_credits

# Function to plot credits usage
# Function to plot credits usage
def plot_credits_usage(daily_credits_df, daily_credits_used):
    fig, ax = plt.subplots(figsize=(20, 6))
    ax.bar(daily_credits_df['usage_date'], daily_credits_used)
    ax.set_xlabel("Date")
    ax.set_ylabel("Credits Used")
    ax.set_title("Daily Credits Usage for " + daily_credits_df['usage_date'].iloc[0].strftime("%B %Y"))

    # Format x-axis labels to display month name
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    plt.xticks(rotation=45)
    
    # Set y-axis limits to range from 0 to 10
    ax.set_ylim(0, 10)
    
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    return fig

# users data!!!
def get_users():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW USERS")
        users_data = cur.fetchall()
        return users_data
    except Exception as e:
        raise e
    finally:
        cur.close()
        conn.close()
