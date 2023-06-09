# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-124df56aef51-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/pinterest-bucket"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_pin = "/mnt/pinterest-bucket/topics/124df56aef51.pin/partition=0/*.json" 
file_location_geo = "/mnt/pinterest-bucket/topics/124df56aef51.geo/partition=0/*.json" 
file_location_user = "/mnt/pinterest-bucket/topics/124df56aef51.user/partition=0/*.json" 
file_type = "json"

# Ask Spark to infer the schema
infer_schema = "true"

# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_pin)

df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_geo)

df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_user)

# Display Spark dataframe to check its content
display(df_pin)
display(df_geo)
display(df_user)

# COMMAND ----------

def get_follower_count_multiplier(df):
    return df.withColumn("multiplier",\
    "1" * \
    regexp_replace(\
        regexp_replace(\
            regexp_replace(col("follower_count"), "[0123456789]", ""),\
            "[k]", "1000"),\
        "[M]", "1000000")\
    )\
    .na.fill(value=1,subset=["multiplier"])

def follower_count_to_int(df):
    return df.withColumn("follower_count", regexp_replace(col("follower_count"), "[A-Za-z]", "").cast("int") * col("multiplier"))

df_pin = df_pin.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_pin.columns])\
    .withColumn("description", when(col("description")=="No description available Story format" ,None).otherwise(col("description")))\
    .withColumn("follower_count", when(col("follower_count")=="User Info Error" ,None).otherwise(col("follower_count")))\
    .transform(get_follower_count_multiplier)\
    .transform(follower_count_to_int)\
    .drop("multiplier")\
    .withColumn("downloaded", col("downloaded").cast("int"))\
    .withColumn("index", col("index").cast("int"))\
    .withColumnRenamed("index", "ind")\
    .withColumn("save_location", regexp_replace(col("save_location"), "Local save in ", ""))\
    .withColumn("image_src", when(col("image_src")=="Image src error." ,None).otherwise(col("image_src")))\
    .withColumn("tag_list", when(col("tag_list")=="N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e" ,None).otherwise(col("tag_list")))\
    .withColumn("title", when(col("title")=="No Title Data Available" ,None).otherwise(col("title")))

df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")


display(df_pin)
 