from sqlalchemy import create_engine
import urllib
from urllib.parse import quote_plus
import configparser


def load_to_ssms_database(pdf, table_name):
    config = configparser.ConfigParser()
    config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')

    mssql_server = config['ssms']['SERVER']
    mssql_database = config['ssms']['DATABASE']
    mssql_username = config['ssms']['UID']
    mssql_password = config['ssms']['PWD']

    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={mssql_server};"
        f"DATABASE={mssql_database};"
        f"UID={mssql_username};"
        f"PWD={mssql_password};"
        f"TrustServerCertificate=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    pdf.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',  # Or 'append'
        index=False
    )
