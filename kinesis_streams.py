# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
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
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

streaming_df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124df56aef51-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

streaming_df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124df56aef51-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

streaming_df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-124df56aef51-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()



# COMMAND ----------

df_pin_json = streaming_df_pin.selectExpr("CAST(data as STRING)")
df_geo_json = streaming_df_geo.selectExpr("CAST(data as STRING)")
df_user_json = streaming_df_user.selectExpr("CAST(data as STRING)")

pin_schema = StructType([ \
    StructField("index",StringType(),True), \
    StructField("unique_id",StringType(),True), \
    StructField("title",StringType(),True), \
    StructField("description", StringType(), True), \
    StructField("poster_name", StringType(), True), \
    StructField("follower_count", StringType(), True), \
    StructField("tag_list", StringType(), True), \
    StructField("is_image_or_video", StringType(), True), \
    StructField("image_src", StringType(), True), \
    StructField("downloaded", IntegerType(), True), \
    StructField("save_location", StringType(), True), \
    StructField("category", StringType(), True) \
  ])

geo_schema = StructType([ \
    StructField("ind",StringType(),True), \
    StructField("timestamp",StringType(),True), \
    StructField("latitude", FloatType(),True), \
    StructField("longitude", FloatType(), True), \
    StructField("country", StringType(), True) \
  ])

user_schema = StructType([ \
    StructField("ind",StringType(),True), \
    StructField("first_name",StringType(),True), \
    StructField("last_name",StringType(),True), \
    StructField("age", IntegerType(), True), \
    StructField("date_joined", StringType(), True)
  ])

df_pin = df_pin_json.withColumn("data", from_json(df_pin_json.data, pin_schema)).select(col("data.*"))
df_geo = df_geo_json.withColumn("data", from_json(df_geo_json.data, geo_schema)).select(col("data.*"))
df_user = df_user_json.withColumn("data", from_json(df_user_json.data, user_schema)).select(col("data.*"))


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

df_pin_clean = df_pin.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_pin.columns])\
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

df_pin_clean = df_pin_clean.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

display(df_pin_clean)
 

# COMMAND ----------

df_geo_clean = df_geo.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_geo.columns])\
    .withColumn("coordinates",array(col("latitude"), col("longitude")))\
    .drop("latitude")\
    .drop("longitude")\
    .withColumn("timestamp", to_timestamp("timestamp", 'MM/dd/yyyy, HH:mm:ss'))\
    .withColumn("ind", col("ind").cast("int"))

df_geo_clean = df_geo_clean.select("ind", "country", "coordinates", "timestamp")

display(df_geo_clean)

# COMMAND ----------

df_user_clean = df_user.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_user.columns])\
    .withColumn("user_name",concat_ws(" ", col("first_name"), col("last_name")))\
    .drop("first_name")\
    .drop("last_name")\
    .withColumn("date_joined", to_timestamp("date_joined", 'MM/dd/yyyy, HH:mm:ss'))\
    .withColumn("ind", col("ind").cast("int"))\
    .withColumn("age", col("age").cast("int"))

df_user_clean = df_user_clean.select("ind", "user_name", "age", "date_joined")

display(df_user_clean)

# COMMAND ----------

df_pin_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("124df56aef51_pin_table")

# COMMAND ----------

df_geo_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("124df56aef51_geo_table")


# COMMAND ----------

df_user_clean.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("124df56aef51_user_table")
