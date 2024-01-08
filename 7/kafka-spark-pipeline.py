import argparse
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql import types as t
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import os

def process_batch(df, batch_id):
    df.write \
      .format("parquet") \
      .mode("append") \
      .save("df.parquet")
    
def main(args):
    IP = args.ip
    read_options = {
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol" : "PLAINTEXT",
        "kafka.bootstrap.servers": f'{IP}:9092',
        "group.id": 'train_group',
        "subscribe": 'train',
        "startingOffsets": "earliest",
    }

    write_kafka_params = {
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol" : "PLAINTEXT",
        "kafka.bootstrap.servers": f'{IP}:9092',
        "topic": "predict"
    }

    schema = t.StructType(
        [
            t.StructField('X', t.ArrayType(t.DoubleType()), True),
            t.StructField('y', t.FloatType(), True),
        ],
    )

    spark = SparkSession\
        .builder\
        .appName("MySparkApp")\
        .config("spark.jars.packages", 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3')\
        .config("spark.sql.streaming.checkpointLocation", "/tmp/ubuntu/checkpoint")\
        .getOrCreate()

    df = spark \
    .readStream \
    .format("kafka") \
    .options(**read_options) \
    .load()

    df = (df.selectExpr('CAST(value AS STRING)') \
        .select(from_json('value', schema).alias('raw_data')))

    df = df.select('raw_data.y', *[col('raw_data.X').getItem(i).alias(f'X{i+1}') for i in range(0, 3)])

    vector_assembler = VectorAssembler(inputCols=["X1", "X2", "X3"], outputCol="features")
    vectorized_data = vector_assembler.transform(df)

    vectorized_data.writeStream \
        .foreachBatch(process_batch) \
        .start() \
        .awaitTermination(3)

    train = spark.read.parquet("df.parquet")

    linear_regression = LinearRegression(featuresCol="features", labelCol="y")
    model = linear_regression.fit(train)

    # Предсказание на потоковых данных с помощью модели
    predictions = model.transform(vectorized_data)

    # Определение настроек для записи данных в Kafka

    write_kafka_params = {
        # "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="mlops" password="mlops_pw";',
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol" : "PLAINTEXT",
        "kafka.bootstrap.servers": '158.160.70.184:9092',
        "topic": "test"
    }


    # Запись предсказаний в Kafka
    stream_writer = predictions.select(col("prediction").alias("value").cast(t.StringType()))\
        .writeStream \
        .format("kafka") \
        .outputMode("append") \
        .options(**write_kafka_params) \
        .start()

    # Запуск потока обработки
    stream_writer.awaitTermination()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Kafka test")

    parser.add_argument("-i", "--ip", type=str, help="Kafka broker host ip")
    args = parser.parse_args()

    main(args)