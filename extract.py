import configparser
import boto3
from pyspark.sql import SparkSession

def connect():
    spark = (
        SparkSession.builder
        .appName("S3ToMSSQL_ETL")
        .config(
            "spark.jars",
            "file:///C:/jars/hadoop-aws-3.3.4.jar,"
            "file:///C:/jars/aws-java-sdk-bundle-1.11.901.jar"
        )
        .getOrCreate()
    )

    config = configparser.ConfigParser()
    config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')

    aws_access_key_id = config['AWS']['aws_access_key_id']
    aws_secret_access_key = config['AWS']['aws_secret_access_key']
    region = config['AWS']['region_2']
    bucket_name = config['AWS']['bucket_parquet']

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_access_key)
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{region}.amazonaws.com")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    response = s3_client.list_objects_v2(Bucket=bucket_name)

    return response, bucket_name, spark
