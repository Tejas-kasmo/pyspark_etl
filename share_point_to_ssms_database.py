import os
from io import BytesIO
import configparser
from pyspark.sql import SparkSession
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from sqlalchemy import create_engine
import urllib.parse

spark = SparkSession.builder.appName("Read_Parquet_Example").master("local[*]").getOrCreate()

config = configparser.ConfigParser()
config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')

site_url = config['SharePoint']['url']
username = config['SharePoint']['username']
password = config['SharePoint']['password']

sql_username = config['ssms']['UID']
sql_password = config['ssms']['PWD']
sql_server = config['ssms']['SERVER']
sql_database = config['ssms']['DATABASE']

encoded_username = urllib.parse.quote_plus(sql_username)
encoded_password = urllib.parse.quote_plus(sql_password)

connection_string = (
    f"mssql+pyodbc://{encoded_username}:{encoded_password}@{sql_server}/{sql_database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(connection_string)

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

folder_url = "/sites/kasmo-training/Shared Documents/DATASET/Parquet_Files"
folder = ctx.web.get_folder_by_server_relative_url(folder_url)

files = folder.files
ctx.load(files)
ctx.execute_query()

print(f"Number of files found: {len(files)}")

current_dir = os.path.dirname(os.path.abspath(__file__))

for file in files:
    file_name = file.properties['Name']
    file_url = file.properties['ServerRelativeUrl']

    print(f"Downloading: {file_name}")

    file_obj = ctx.web.get_file_by_server_relative_url(file_url)

    download_buffer = BytesIO()
    file_obj.download(download_buffer).execute_query()

    local_file_path = os.path.join(current_dir, file_name)
    with open(local_file_path, 'wb') as f:
        f.write(download_buffer.getvalue())

    print(f"Saved locally: {local_file_path}")

    df = spark.read.parquet(local_file_path)

    table_name = file_name.replace('.parquet', '').replace(' ', '_')
    df.toPandas().to_sql(table_name, con=engine, if_exists='replace', index=False)

    print(f"Uploaded {table_name} to SQL Server")

spark.stop()
print(" All files uploaded to SSMS database.")
