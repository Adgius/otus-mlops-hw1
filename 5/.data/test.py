from pyspark.sql import SparkSession, SQLContext

spark = SparkSession\
        .builder\
        .appName("mytestapp")\
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:1.27.0")\
        .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")\
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .getOrCreate()
sql = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

import mlflow

print(mlflow.__version__)