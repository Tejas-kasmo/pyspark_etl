import os
from io import BytesIO
import configparser
import boto3
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

config = configparser.ConfigParser()
config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')

site_url = config['SharePoint']['url']
username = config['SharePoint']['username']
password = config['SharePoint']['password']

aws_access_key_id = config['AWS']['aws_access_key_id']
aws_secret_access_key = config['AWS']['aws_secret_access_key']
region = config['AWS']['region']
bucket_name = config['AWS']['bucket_parquet']

ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

folder_url = "/sites/kasmo-training/Shared Documents/DATASET/Parquet_Files"
folder = ctx.web.get_folder_by_server_relative_url(folder_url)

files = folder.files
ctx.load(files)
ctx.execute_query()

print(f"Number of files found: {len(files)}")

current_dir = os.path.dirname(os.path.abspath(__file__))

s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region
)

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

    s3_key = f"{file_name}"
    s3_client.upload_file(local_file_path, bucket_name, s3_key)

    print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

    os.remove(local_file_path)
    print(f"Deleted local copy: {local_file_path}")

print("All files uploaded to S3 and local copies cleaned up.")
