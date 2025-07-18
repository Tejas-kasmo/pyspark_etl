import extract
import transform
import load

print("data extracting")
responses, bucket_name, spark = extract.connect()
print("data extracted")

print("doing type conversions")
pandas_df, table_name = transform.type_conversion(responses, bucket_name, spark)
print("type conversions done")

print("loading to ssms database")
load.load_to_ssms_database(pandas_df, table_name)
print("data loaded")
