class Config:
    SNOWFLAKE_USER = 'Nithin'
    SNOWFLAKE_PASSWORD = 'Nithin@2024'
    SNOWFLAKE_ACCOUNT = 'bdhriyc-ke24872'
    SNOWFLAKE_DATABASE = 'INVENTORY'
    SNOWFLAKE_SCHEMA = 'PUBLIC'

    @staticmethod
    def as_dict():
        return {
            'user': Config.SNOWFLAKE_USER,
            'password': Config.SNOWFLAKE_PASSWORD,
            'account': Config.SNOWFLAKE_ACCOUNT,
            'database': Config.SNOWFLAKE_DATABASE,
            'schema': Config.SNOWFLAKE_SCHEMA
        }