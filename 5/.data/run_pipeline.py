# coding: utf8

import argparse
import os

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, Transformer

from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from functools import reduce
from typing import Iterable
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from datetime import datetime
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.util import MLReadable, MLWritable

import mlflow
from mlflow.tracking import MlflowClient

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

import boto3
import warnings
warnings.simplefilter('ignore')


features = ['tx_amount', 'tx_time_seconds_prev', 'tx_time_seconds_duration', 'tx_amount_prev', 
'tx_amount_duration', 'tx_time_seconds_diff', 'tx_amount_diff', 
'tx_time_seconds_mean', 'tx_amount_mean', 'terminal_id_prev', 'terminal_id_amount_mean', 'terminal_id_time_mean']
target = 'tx_fraud'
attributes = features + [target]

class FeatureGenerator(Transformer, MLReadable, MLWritable):

    #def __init__(self, features: Iterable[str]):
    def __init__(self, features):
        super(FeatureGenerator, self).__init__()
        self.features = features

    #def _transform(self, df: DataFrame) -> DataFrame:     
    def _transform(self, df):                   
        w1 = Window().partitionBy("customer_id").orderBy(F.asc("tx_time_seconds"))
        w2 = Window().partitionBy("terminal_id").orderBy(F.asc("tx_time_seconds"))
        w3 = Window().partitionBy('customer_id').orderBy(F.asc('tx_time_seconds')).rowsBetween(1, 3)
                           
        df = df.withColumn("tx_time_seconds_prev", F.lag("tx_time_seconds").over(w1))
        df = df.withColumn('tx_time_seconds_duration',  F.col('tx_time_seconds') - F.col('tx_time_seconds_prev'))

        df = df.withColumn("tx_amount_prev", F.lag("tx_amount").over(w1))
        df = df.withColumn('tx_amount_duration',  F.col('tx_amount') - F.col('tx_amount_prev'))

        df = df.withColumn('tx_time_seconds_diff',  F.col('tx_time_seconds_duration') / F.col('tx_time_seconds_prev'))
        df = df.withColumn('tx_amount_diff',  F.col('tx_amount_duration') / F.col('tx_amount_prev'))

        # Сколько в среднем за последние 3 транзакции
        df = df.withColumn('tx_time_seconds_mean',  F.mean('tx_time_seconds_duration').over(w3))
        df = df.withColumn('tx_amount_mean',  F.mean('tx_amount_duration').over(w3))      

        # Тот же терминал
        df = df.withColumn("terminal_id_prev", F.lag("terminal_id").over(w2))
        df = df.withColumn('terminal_id_prev',  (F.col('terminal_id') == F.col('terminal_id_prev')).cast('integer'))

        # Какая в средняя стата по терминалу у клиента
        df = df.withColumn("terminal_id_amount_mean", F.mean("tx_amount_duration").over(w2))
        df = df.withColumn("terminal_id_time_mean", F.mean("tx_time_seconds_duration").over(w2))
             
        return df
    

def get_pipeline():
    generator = FeatureGenerator(features=attributes)
    imputer = Imputer(inputCols=features, outputCols=features)
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    scaler = StandardScaler(inputCol='features', outputCol='features_scaled')
    lr = LogisticRegression(featuresCol='features_scaled', labelCol='tx_fraud')
    pipeline = Pipeline(stages=[generator, imputer, assembler, scaler, lr])
    return pipeline

def set_env(args):
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.MLFLOW_S3_ENDPOINT_URL
    os.environ['AWS_DEFAULT_REGION'] = args.AWS_DEFAULT_REGION
    os.environ['AWS_ACCESS_KEY_ID'] = args.AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.AWS_SECRET_ACCESS_KEY
    os.environ['MLFLOW_ARTIFACT_URI'] = args.MLFLOW_ARTIFACT_URI

def main(args):

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

    # If experiment does not exist
    try:
        mlflow.create_experiment('baseline')
    except:
        pass

    # Prepare MLFlow experiment for logging
    client = MlflowClient()
    experiment = client.get_experiment_by_name("baseline")
    experiment_id = experiment.experiment_id
    run_name = 'My run name' + ' ' + str(datetime.now())

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
            dfs.append(sql.read.parquet('s3a://{}/{}'.format(input_bucket, data.key)).limit(100000))
            break
        df = reduce(DataFrame.unionAll, dfs)

        logger.info("Getting new pipeline ...")
        inf_pipeline = get_pipeline()
        
        lr = inf_pipeline.getStages()[-1]
        
        paramGrid =  ParamGridBuilder() \
                    .addGrid(lr.regParam, [1.0, 2.0]) \
                    .addGrid(lr.maxIter, [50, 100]) \
                    .build()

        evaluator = BinaryClassificationEvaluator(labelCol=target)

        # By default 80% of the data will be used for training, 20% for validation.
        trainRatio = 1 - val_frac
                
        # A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
        tvs = TrainValidationSplit(estimator=inf_pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, parallelism=1, seed=42)

        # Run TrainValidationSplit, and choose the best set of parameters.
        logger.info("Fitting new inference pipeline ...")
        model = tvs.fit(df)

        # Log params, metrics and model with MLFlow

        run_id = mlflow.active_run().info.run_id
        logger.info("Logging optimal parameters to MLflow run {} ...".format(run_id))

        best_regParam = model.bestModel.stages[-1].getRegParam()
        best_fitIntercept = model.bestModel.stages[-1].getFitIntercept()
        best_elasticNetParam = model.bestModel.stages[-1].getElasticNetParam()

        logger.info(model.bestModel.stages[-1].explainParam('regParam'))
        logger.info(model.bestModel.stages[-1].explainParam('fitIntercept'))
        logger.info(model.bestModel.stages[-1].explainParam('elasticNetParam'))

        mlflow.log_param('optimal_regParam', best_regParam)
        mlflow.log_param('optimal_fitIntercept', best_fitIntercept)
        mlflow.log_param('optimal_elasticNetParam', best_elasticNetParam)

        logger.info("Scoring the model ...")
        predictions = model.transform(df)
        metric = evaluator.evaluate(predictions)
        logger.info("Logging metrics to MLflow run {} ...".format(run_id))
        mlflow.log_metric("areaUnderROC", metric)
        logger.info("Model areaUnderROC: {}".format(metric))

        logger.info("Saving model ...")
        mlflow.spark.save_model(model.bestModel.stages[-1], output_artifact)

        logger.info("Exporting/logging model ...")
        mlflow.spark.log_model(model.bestModel.stages[-1], output_artifact)
        logger.info("Done")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Model (Inference Pipeline) Training")

    parser.add_argument("-f", "--val_frac", type=float, default=0.2, help="Size of the validation split. Fraction of the dataset.")
    parser.add_argument("-i", "--input_bucket", type=str, default ='otus-mlops-data-clear', help="Name for the bucket with input data")
    parser.add_argument("-o", "--output_artifact", type=str, help="Name for the output serialized model (Inference Artifact folder)" ,required=True,)
    parser.add_argument('-u', '--mlflow_tracking_uri', type=str, help='mlflow_tracking_uri')
    parser.add_argument('-r', '--AWS_DEFAULT_REGION', type=str, help='AWS_DEFAULT_REGION')
    parser.add_argument('-a', '--MLFLOW_ARTIFACT_URI', type=str, help='MLFLOW_ARTIFACT_URI')
    parser.add_argument('-e', '--MLFLOW_S3_ENDPOINT_URL', type=str, default ='s3a://mlflow-otus-test', help='MLFLOW_S3_ENDPOINT_URL')
    parser.add_argument('-k', '--AWS_ACCESS_KEY_ID', type=str, help='S3 AWS_ACCESS_KEY_ID')
    parser.add_argument('-s', '--AWS_SECRET_ACCESS_KEY', type=str, help='S3 AWS_SECRET_ACCESS_KEY')
    args = parser.parse_args()

    main(args)