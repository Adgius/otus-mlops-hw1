import argparse
import os
parser = argparse.ArgumentParser(description='credentials')
parser.add_argument('aws_access_key_id', type=str, help='aws_access_key_id')
parser.add_argument('aws_secret_access_key', type=str, help='aws_secret_access_key')
args = parser.parse_args()

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, FloatType, IntegerType, StructField, StructType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.window import Window


import boto3
import datetime as dt
import warnings

warnings.simplefilter('ignore')
s3 = boto3.resource('s3',
                endpoint_url='https://storage.yandexcloud.net',
                region_name = 'ru-central1',
                aws_access_key_id = args.aws_access_key_id,
                aws_secret_access_key = args.aws_secret_access_key
                )

data_bucket = s3.Bucket('mlops-data')


try:
    sc = SparkContext()
except ValueError:
    sc = SparkContext.getOrCreate()
    
if sc:
    sc.stop()
    
sc = SparkContext()

spark = SparkSession\
        .builder\
        .appName("mytestapp")\
        .config("fs.s3a.endpoint", "https://storage.yandexcloud.net")\
        .config("fs.s3a.access.key", args.aws_access_key_id)\
        .config("fs.s3a.secret.key", args.aws_secret_access_key)\
        .config("fs.s3a.acl.default", "BucketOwnerFullControl")\
        .getOrCreate()

sql = SQLContext(spark)

def fix_date(d):
    try:
        date, time = d.split()
        Y = int(date.split('-')[0])
        m = int(date.split('-')[1])
        d = int(date.split('-')[2])
        H = int(time.split(':')[0])
        M = int(time.split(':')[1])
        S = int(time.split(':')[2])
        if H == 24:
            H = 23
        return (dt.datetime(year=Y, month=m, day=d, hour=H, minute=M, second=S) + dt.timedelta(hours=1))
    except:
        return float('nan')
    
def try_convert(x, func):
    try:
        return func(x)
    except:
        return float('nan')

def read_csv(s3obj):
    rdd = sc.textFile(os.path.join("s3a://" , s3obj.bucket_name, s3obj.key))
    bad_header =  rdd.first()
    rdd = rdd.filter(lambda line: line != bad_header)
    temp_var = rdd.map(lambda row: row.split(","))
    temp_var = temp_var.map(lambda row: (
                                     try_convert(row[0], int),
                                     fix_date(row[1]), 
                                     try_convert(row[2], float),
                                     try_convert(row[3], float), 
                                     try_convert(row[4], float),
                                     try_convert(row[5], float),
                                     try_convert(row[6], float),
                                     try_convert(row[7], float),
                                     try_convert(row[8], float))
                       )
    schema = StructType([StructField('tranaction_id', IntegerType(), True),
                         StructField('tx_datetime', TimestampType(), True),
                         StructField('customer_id', FloatType(), True),
                         StructField('terminal_id', FloatType(), True),
                         StructField('tx_amount', FloatType(), True),
                         StructField('tx_time_seconds', FloatType(), True),
                         StructField('tx_time_days', FloatType(), True),
                         StructField('tx_fraud', FloatType(), True),
                         StructField('tx_fraud_scenario', FloatType(), True),])
    return spark.createDataFrame(temp_var, schema)

def find_time_outliers(df, n=4):
    df = df.withColumn('tx_datetime', F.unix_timestamp(df.tx_datetime))
    assembler = VectorAssembler(inputCols=['tx_datetime'],
                                outputCol='features')
    df = assembler.transform(df) # Чтоб быстрее считалась
    train = df.limit(10000)

    lin_reg = LinearRegression(
        featuresCol='features', 
        labelCol='tx_time_seconds') 

    lrModel = lin_reg.fit(train)
    df = lrModel.transform(df)
    df = df.withColumn('residuals', F.col('tx_time_seconds') - F.col('prediction'))
    std = df.select(F.stddev('residuals')).head()[0]
    
    border = 4 * std
    
    df = df.withColumn('tx_time_seconds', 
                               F.when(F.col('residuals') > border, F.col('prediction'))\
                              .otherwise(F.col('tx_time_seconds')))
    df = df.drop(*['features', 'prediction', 'residuals'])
    return df

def clear_data(df):
    tx_amount_q99 = 131.45
    time = dt.datetime.now()
    df = df.dropna(thresh=2)
    df = df.withColumn('tx_amount', 
                           F.when(F.col('tx_amount') > tx_amount_q99, tx_amount_q99)\
                          .otherwise(F.col('tx_amount')))
    df = find_time_outliers(df)
    print('Затрачено минут:', (dt.datetime.now() - time).seconds / 60)
    return df

for data in data_bucket.objects.all():
    print(f'========= {data.key} ========')
    df = read_csv(data)
    df = clear_data(df)
    filename = data.key.replace('.txt', '.parquet')
    df.coalesce(1).write.format('parquet').save(f"s3a://test-4-otus/{data.key.replace('.txt', '.parquet')}", mode='overwrite')


for data in data_bucket.objects.all():
    if '_SUCCESS' not in data.key:
        s3.meta.client.copy({'Bucket': 'test-4-otus','Key': data.key}, 'test-4-otus', data.key.split("/")[1])
        s3.Object('test-4-otus', data.key).delete()
    else:
        s3.Object('test-4-otus', data.key).delete()