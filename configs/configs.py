import os
from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.sql.shuffle.partitions", 6)
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .appName("tranform")
    .getOrCreate()
)
