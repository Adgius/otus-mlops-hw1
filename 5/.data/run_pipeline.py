import argparse
import os
import sys

import pyspark.sql.types as T
import pyspark.sql.functions as F
import mlflow
import logging
import boto3
import warnings

from functools import reduce
import datetime as dt

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator, Evaluator
from pyspark.ml.util import MLReadable, MLWritable


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


warnings.simplefilter('ignore')

logger.info("Python path: {}".format(sys.executable))
logger.info("Python version: {}".format(sys.version))

features = ['tx_amount', 'tx_time_seconds', 'tx_time_days',
            'weekend', 'day_night', 'avg_transaction_count_1',
            'avg_transaction_count_7', 'avg_transaction_count_30', 'avg_transaction_mean_1',
            'avg_transaction_mean_7', 'avg_transaction_mean_30', 'avg_transaction_terminal_id_count_1',
            'avg_transaction_terminal_id_count_7', 'avg_transaction_terminal_id_count_30',
            "sum_fraud_terminal_id_count_1", "sum_fraud_terminal_id_count_7", "sum_fraud_terminal_id_count_30"]
target = 'tx_fraud'
attributes = features + [target]

class FNR_metric(Evaluator):

    def __init__(self, predictionCol='prediction', labelCol='tx_fraud'):
        self.predictionCol = predictionCol
        self.labelCol = labelCol

    def _evaluate(self, dataset):
        tp = dataset.filter((F.col(self.labelCol) == 1) & (F.col(self.predictionCol) == 1)).count()
        fn = dataset.filter((F.col(self.labelCol) == 1) & (F.col(self.predictionCol) == 0)).count()
        fnr = fn / (fn + tp)
        return fnr

    def isLargerBetter(self):
        return True
    
class FPR_metric(Evaluator):

    def __init__(self, predictionCol='prediction', labelCol='tx_fraud'):
        self.predictionCol = predictionCol
        self.labelCol = labelCol

    def _evaluate(self, dataset):
        tn = dataset.filter((F.col(self.labelCol) == 0) & (F.col(self.predictionCol) == 0)).count()
        fp = dataset.filter((F.col(self.labelCol) == 0) & (F.col(self.predictionCol) == 1)).count()
        fpr = fp / (fp + tn)
        return fpr

    def isLargerBetter(self):
        return True

class DataFixer(Transformer):

    def __init__(self):
        super(DataFixer, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:
        """
        Здесь можно описать исправления для данных перед генерацией новых признаков
        """
        df = df.withColumn('tx_datetime', fix_date_udf(F.col('tx_datetime')))


        return df

class FeatureGenerator(Transformer, MLReadable, MLWritable):

    def __init__(self):
        super(FeatureGenerator, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:
        days = lambda i: i * 86400  # Hive timestamp is interpreted as UNIX timestamp in seconds*
        # выходной день
        df = df.withColumn('weekend', F.when(F.dayofweek(F.col('tx_datetime')).isin([5,6]), 1).otherwise(0))

        # время суток
        df = df.withColumn('day_night', F.when(F.hour(F.col('tx_datetime')) < 9, 1).otherwise(0))

        w1 = Window().partitionBy("customer_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(1), 0)
        w2 = Window().partitionBy("customer_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(7), 0)
        w3 = Window().partitionBy("customer_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(30), 0)

        w4 = Window().partitionBy("terminal_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(1), 0)
        w5 = Window().partitionBy("terminal_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(7), 0)
        w6 = Window().partitionBy("terminal_id").orderBy(F.col("tx_datetime").cast("long").asc()).rangeBetween(-days(30), 0)

        # скользящее среднее числа транзакций по каждой карте
        df = df.withColumn("avg_transaction_count_1", F.count("tranaction_id").over(w1))
        df = df.withColumn("avg_transaction_count_7", F.count("tranaction_id").over(w2))
        df = df.withColumn("avg_transaction_count_30", F.count("tranaction_id").over(w3))

        # скользящее среднее суммы транзакций по каждой карте
        df = df.withColumn("avg_transaction_mean_1", F.mean("tx_amount").over(w1))
        df = df.withColumn("avg_transaction_mean_7", F.mean("tx_amount").over(w2))
        df = df.withColumn("avg_transaction_mean_30", F.mean("tx_amount").over(w3))

        # скользящее среднее числа транзакций по каждому терминалу
        df = df.withColumn("avg_transaction_terminal_id_count_1", F.count("terminal_id").over(w4))
        df = df.withColumn("avg_transaction_terminal_id_count_7", F.count("terminal_id").over(w5))
        df = df.withColumn("avg_transaction_terminal_id_count_30", F.count("terminal_id").over(w6))

        # скользящее сумма по числу мошейнических операций на данном терминале
        df = df.withColumn("sum_fraud_terminal_id_count_1", F.sum("tx_fraud").over(w4))
        df = df.withColumn("sum_fraud_terminal_id_count_7", F.sum("tx_fraud").over(w5))
        df = df.withColumn("sum_fraud_terminal_id_count_30", F.sum("tx_fraud").over(w6))
        return df

def read_csv(s3obj, spark, limit=100000):

    def try_convert(x, func):
        try:
            return func(x)
        except:
            return float('nan')

    rdd = spark.read.text(os.path.join("s3a://" , s3obj.bucket_name, s3obj.key)).rdd
    rdd = spark.sparkContext.parallelize(rdd.take(limit))
    bad_header =  rdd.first()
    rdd = rdd.filter(lambda line: line != bad_header)
    temp_var = rdd.map(lambda row: row.value.split(","))
    temp_var = temp_var.map(lambda row: (
                                     try_convert(row[0], int),
                                     try_convert(row[1], str), 
                                     try_convert(row[2], int),
                                     try_convert(row[3], int), 
                                     try_convert(row[4], float),
                                     try_convert(row[5], int),
                                     try_convert(row[6], int),
                                     try_convert(row[7], int),
                                     try_convert(row[8], int))
                       )
    schema = T.StructType([T.StructField('tranaction_id', T.LongType(), True),
                         T.StructField('tx_datetime', T.StringType(), True),
                         T.StructField('customer_id', T.LongType(), True),
                         T.StructField('terminal_id', T.LongType(), True),
                         T.StructField('tx_amount', T.DoubleType(), True),
                         T.StructField('tx_time_seconds', T.LongType(), True),
                         T.StructField('tx_time_days', T.LongType(), True),
                         T.StructField('tx_fraud', T.LongType(), True),
                         T.StructField('tx_fraud_scenario', T.LongType(), True),])
    return spark.createDataFrame(temp_var, schema) 

def fix_date(d, verbose=False):
    try:
        date, time = d.split()
        Y = date.split('-')[0]
        m = date.split('-')[1]
        d = date.split('-')[2]
        H = time.split(':')[0]
        M = time.split(':')[1]
        S = time.split(':')[2]
        if H == '24':
            H = '23'
            data = dt.datetime.strptime(f'{Y}-{m}-{d} {H}:{M}:{S}', '%Y-%m-%d %H:%M:%S') + dt.timedelta(hours=1)
        else:
            data = dt.datetime.strptime(f'{Y}-{m}-{d} {H}:{M}:{S}', '%Y-%m-%d %H:%M:%S')
        return data.replace(tzinfo=pytz.timezone('Europe/Moscow'))
    except:
        return None

fix_date_udf = F.udf(lambda x: fix_date(x), T.TimestampType())    

def get_pipeline():
    fixer = DataFixer()
    generator = FeatureGenerator()
    imputer = Imputer(inputCols=features, outputCols=features)
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    scaler = StandardScaler(inputCol='features', outputCol='features_scaled')
    pipeline = Pipeline(stages=[fixer, generator, imputer, assembler, scaler])
    return pipeline

def set_env(args):
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.MLFLOW_S3_ENDPOINT_URL
    os.environ['AWS_DEFAULT_REGION'] = args.AWS_DEFAULT_REGION
    os.environ['AWS_ACCESS_KEY_ID'] = args.AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.AWS_SECRET_ACCESS_KEY
    os.environ['MLFLOW_ARTIFACT_URI'] = args.MLFLOW_ARTIFACT_URI
    # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

def main(args):
    
    logger.info("Add credentials to environment ...")
    set_env(args)

    input_bucket = args.input_bucket
    output_artifact = args.output_artifact
    val_frac = args.val_frac

    s3 = boto3.resource('s3',
                    endpoint_url='https://storage.yandexcloud.net',
                    region_name = 'ru-central1',
                    aws_access_key_id = args.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key = args.AWS_SECRET_ACCESS_KEY
                    )
    data_bucket = s3.Bucket(input_bucket)

    mlflow.set_tracking_uri('http://{}:5000'.format(args.mlflow_tracking_uri))

    logger.info("Create experiment ...")
    try:
        mlflow.create_experiment('baseline')
    except:
        logger.info("Experiment already exists")

    # Prepare MLFlow experiment for logging
    experiment = mlflow.get_experiment_by_name('baseline')
    experiment_id = experiment.experiment_id
    run_name = 'My run name' + ' ' + str(dt.datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})

        logger.info("Creating Spark Session ...")
        spark = SparkSession\
                .builder\
                .appName("mytestapp")\
                .config("spark.jars.packages", "org.mlflow:mlflow-spark:1.27.0")\
                .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34")\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")\
                .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                .getOrCreate()
        sql = SQLContext(sparkContext=spark.sparkContext, sparkSession=spark)

        logger.info("Loading Data ...")
        dfs = []
        for data in data_bucket.objects.all():
            dfs.append(read_csv(data, spark, limit=100000))

        df = reduce(DataFrame.unionAll, dfs)

        logger.info("Getting new pipeline ...")
        transform_pipeline = get_pipeline()
        
        df = transform_pipeline.fit(df).transform(df)

        clf = DecisionTreeClassifier(featuresCol='features_scaled', labelCol='tx_fraud')
        
        paramGrid =  ParamGridBuilder() \
                    .addGrid(DecisionTreeClassifier().maxDepth, [2, 4]) \
                    .addGrid(DecisionTreeClassifier().minInstancesPerNode , [2, 5]) \
                    .build()

        evaluator = BinaryClassificationEvaluator(labelCol=target)

        # By default 80% of the data will be used for training, 20% for validation.
        trainRatio = 1 - val_frac
        
        logger.info("Upsampling train dataset ...")        
        train, test = df.randomSplit([trainRatio, val_frac])
        train_0 = train.where(F.col(target)==0)
        train_1 = train.where(F.col(target)==1).sample(True, float(train.where(F.col(target)==0).count() / train.where(F.col(target)==1).count()))
        train = train_0.union(train_1)
        train.cache(), test.cache()

        # Run TrainValidationSplit, and choose the best set of parameters.
        logger.info("Fitting new inference pipeline ...")
        cv = CrossValidator(estimator=clf,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator)
        cv_model = cv.fit(train)

        # Log params, metrics and model with MLFlow

        run_id = mlflow.active_run().info.run_id
        logger.info("Logging optimal parameters to MLflow run {} ...".format(run_id))

        best_MaxDepth = cv_model.bestModel.getMaxDepth()
        best_MinInstancesPerNode = cv_model.bestModel.getMinInstancesPerNode()

        logger.info(cv_model.bestModel.explainParam('maxDepth'))
        logger.info(cv_model.bestModel.explainParam('minInstancesPerNode'))

        mlflow.log_param('optimal_maxDepth', best_MaxDepth)
        mlflow.log_param('optimal_MinInstancesPerNode', best_MinInstancesPerNode)

        logger.info("Scoring the model ...")
        predictions = cv_model.transform(test)
        fpr = FPR_metric().evaluate(predictions)
        fnr = FNR_metric().evaluate(predictions)
        logger.info("FPR: {}".format(fpr))
        logger.info("FNR: {}".format(fnr))


        logger.info("Logging metrics to MLflow run {} ...".format(run_id))
        mlflow.log_metric("FPR", fpr)
        mlflow.log_metric("FNR", fnr)

        logger.info("Saving model ...")
        mlflow.spark.save_model(model.bestModel.stages[-1], output_artifact)

        logger.info("Exporting/logging model ...")
        mlflow.spark.log_model(model.bestModel.stages[-1], output_artifact)
        logger.info("Done")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Model (Inference Pipeline) Training")

    parser.add_argument("-f", "--val_frac", type=float, default=0.2, help="Size of the validation split. Fraction of the dataset.")
    parser.add_argument("-i", "--input_bucket", type=str, default ='otus-mlops-hw2', help="Name for the bucket with input data")
    parser.add_argument("-o", "--output_artifact", type=str, help="Name for the output serialized model (Inference Artifact folder)" ,required=True,)
    parser.add_argument('-u', '--mlflow_tracking_uri', type=str, help='mlflow_tracking_uri')
    parser.add_argument('-r', '--AWS_DEFAULT_REGION', type=str, help='AWS_DEFAULT_REGION')
    parser.add_argument('-a', '--MLFLOW_ARTIFACT_URI', type=str, help='MLFLOW_ARTIFACT_URI', default ='s3a://mlflow-otus-test')
    parser.add_argument('-e', '--MLFLOW_S3_ENDPOINT_URL', type=str, help='MLFLOW_S3_ENDPOINT_URL')
    parser.add_argument('-k', '--AWS_ACCESS_KEY_ID', type=str, help='S3 AWS_ACCESS_KEY_ID')
    parser.add_argument('-s', '--AWS_SECRET_ACCESS_KEY', type=str, help='S3 AWS_SECRET_ACCESS_KEY')
    args = parser.parse_args()

    main(args)