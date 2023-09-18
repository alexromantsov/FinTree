import psycopg2
from config import DB_SETTINGS


def create_table(year_columns_ddl):
    connection = psycopg2.connect(**DB_SETTINGS)
    cursor = connection.cursor()
    ddl_query = f"""
    CREATE TABLE IF NOT EXISTS projects (
        code TEXT PRIMARY KEY,
        project_name TEXT,
        {year_columns_ddl}
    );
    """
    cursor.execute(ddl_query)
    connection.commit()
    cursor.close()
    connection.close()
