from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType
import os

def type_conversion(response, bucket_name, spark):

    if 'Contents' in response:
        print("contents in response")
        for obj in response['Contents']:
            print("obj in contents")
            key = obj['Key']
            if key.endswith('.parquet'):
                print(f"\n Processing file: {key}")

                s3_path = f"s3a://{bucket_name}/{key}"

                df = spark.read.parquet(s3_path)

                # Try convert columns
                for column in df.columns:
                    df = df.withColumn(
                        column,
                        when(
                            col(column).cast(IntegerType()).isNotNull(),
                            col(column).cast(IntegerType())
                        ).otherwise(col(column).cast(StringType()))
                    )

                pdf = df.toPandas()

                table_name = os.path.splitext(os.path.basename(key))[0]

                return pdf, table_name
