# coding: utf8

import argparse
import os
import numpy as np
import mlflow

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
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from datetime import datetime
from pyspark.ml.evaluation import BinaryClassificationEvaluator, Evaluator
from pyspark.ml.util import MLReadable, MLWritable
from tqdm import tqdm

from mlflow.tracking import MlflowClient

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

import boto3
import warnings
warnings.simplefilter('ignore')


features = ['tx_amount', 'tx_time_seconds', 'tx_time_days', 'avg_transaction_count_1', 
   'avg_transaction_count_7', 'avg_transaction_count_30', 'avg_transaction_mean_1', 
   'avg_transaction_mean_7', 'avg_transaction_mean_30', 'avg_transaction_terminal_id_count_1', 
    'avg_transaction_terminal_id_count_7', 'avg_transaction_terminal_id_count_30']

target = 'tx_fraud'
attributes = features + [target]

class FeatureGenerator(Transformer, MLReadable, MLWritable):

    def __init__(self):
        super(FeatureGenerator, self).__init__()

    def _transform(self, df: DataFrame) -> DataFrame:  
        days = lambda i: i * 86400  # Hive timestamp is interpreted as UNIX timestamp in seconds*
        
        w1 = Window().partitionBy("customer_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(1), 0)
        w2 = Window().partitionBy("customer_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(7), 0)
        w3 = Window().partitionBy("customer_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(30), 0)
        
        w4 = Window().partitionBy("terminal_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(1), 0)
        w5 = Window().partitionBy("terminal_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(7), 0)
        w6 = Window().partitionBy("terminal_id").orderBy(F.asc("tx_datetime")).rangeBetween(-days(30), 0)
        
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
             
        return df

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
     
def get_pipeline():
    generator = FeatureGenerator()
    imputer = Imputer(inputCols=features, outputCols=features)
    assembler = VectorAssembler(inputCols=features, outputCol='features')
    # scaler = StandardScaler(inputCol='features', outputCol='features_scaled')
    lr = LogisticRegression(featuresCol='features', labelCol='tx_fraud')
    pipeline = Pipeline(stages=[generator, imputer, assembler, lr])
    return pipeline

def set_env(args):
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.MLFLOW_S3_ENDPOINT_URL
    os.environ['AWS_DEFAULT_REGION'] = args.AWS_DEFAULT_REGION
    os.environ['AWS_ACCESS_KEY_ID'] = args.AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.AWS_SECRET_ACCESS_KEY
    os.environ['MLFLOW_ARTIFACT_URI'] = args.MLFLOW_ARTIFACT_URI
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

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

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id) as run:
        run_id = run.info.run_id

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

        logger.info("Getting pipeline ...")
        inf_pipeline = get_pipeline()

        logger.info("Getting model ...")
        lr = inf_pipeline.getStages()[-1]

        logger.info("Getting evaluator ...")
        evaluator = BinaryClassificationEvaluator(labelCol=target)

        logger.info("Getting params grid ...")
        paramGrid =  ParamGridBuilder() \
                    .addGrid(lr.regParam, [1.0, 2.0]) \
                    .addGrid(lr.maxIter, [50, 100]) \
                    .build()
        
        logger.info("Prepare and upsampling data ...")
        train, test = df.randomSplit([1 - val_frac, val_frac])
        train_0 = train.where(F.col(target)==0)
        train_1 = train.where(F.col(target)==1).sample(True, float(train.where(F.col(target)==0).count() / train.where(F.col(target)==1).count()))
        train = train_0.union(train_1)
        train.cache(), test.cache()

        logger.info("Start crossvalidation ...")
        cv = CrossValidator(estimator=inf_pipeline,
                            estimatorParamMaps=paramGrid,
                            evaluator=evaluator)
        cv_model = cv.fit(train)
        best_regParam = cv_model.bestModel.stages[-1].getRegParam()
        best_fitIntercept = cv_model.bestModel.stages[-1].getFitIntercept()
        best_elasticNetParam = cv_model.bestModel.stages[-1].getElasticNetParam()

        logger.info(cv_model.bestModel.stages[-1].explainParam('regParam'))
        logger.info(cv_model.bestModel.stages[-1].explainParam('fitIntercept'))
        logger.info(cv_model.bestModel.stages[-1].explainParam('elasticNetParam'))

        mlflow.log_param('optimal_regParam', best_regParam)
        mlflow.log_param('optimal_fitIntercept', best_fitIntercept)
        mlflow.log_param('optimal_elasticNetParam', best_elasticNetParam)

        logger.info("confidence interval for metric ...")
        fpr = []
        fnr = []
        for _ in tqdm(range(50)):
            predictions = cv_model.transform(test.sample(fraction=1.0, withReplacement=True))
            fpr.append(FPR_metric().evaluate(predictions))
            fnr.append(FNR_metric().evaluate(predictions))
        logger.info(f'FPR: {np.mean(fpr)} CI:[{np.percentile(fpr, 2.5)}, {np.percentile(fpr, 97.5)}]')
        logger.info(f'FNR: {np.mean(fnr)} CI:[{np.percentile(fnr, 2.5)}, {np.percentile(fnr, 97.5)}]')

        mlflow.log_metric('FPR', np.mean(fpr))
        mlflow.log_metric('FPR_lower', np.percentile(fpr, 2.5))
        mlflow.log_metric('FPR_upper', np.percentile(fpr, 97.5))

        mlflow.log_metric('FNR', np.mean(fnr))
        mlflow.log_metric('FNR_lower', np.percentile(fnr, 2.5))
        mlflow.log_metric('FNR_upper', np.percentile(fnr, 97.5))

        logger.info("Saving model ...")
        mlflow.spark.save_model(cv_model.bestModel.stages[-1], output_artifact)

        logger.info("Exporting/logging model ...")
        mlflow.spark.log_model(cv_model.bestModel.stages[-1], output_artifact)

        mlflow.log_metric('best_model', 1)
        logger.info("Done")

        find_best_model = False

        logger.info('Looking for a best model ...')
        for r in client.search_runs(experiment_id):
            r = r.to_dictionary()
            # Check status
            if r['info'].get('status', None) == 'FINISHED':
                # Check is it a best model
                if r['data']['metrics']['best_model'] == 1:
                    FPR_upper_prev = r['data']['metrics']['FPR_upper']
                    FNR_upper_prev = r['data']['metrics']['FNR_upper']
                    if FPR_upper_prev < np.percentile(fpr, 2.5) and float(FNR_upper_prev) < np.percentile(fnr, 2.5):
                        logger.info('New best model ...')
                        find_best_model = True
                        # Set new best model
                        mlflow.log_metric('best_model', 1)
                        # Delete previous best model status
                        client.log_metric(r['info']['run_id'], 'best_model', 0)
                        break
                    else:
                        logger.info('Its not a best model ...')
                        mlflow.log_metric('best_model', 0)
                        break
        
        # if it is a first finished run
        if not find_best_model:
            mlflow.log_metric('best_model', 1)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Model (Inference Pipeline) Training")

    parser.add_argument("-f", "--val_frac", type=float, default=0.2, help="Size of the validation split. Fraction of the dataset.")
    parser.add_argument("-i", "--input_bucket", type=str, default ='otus-mlops-data-clear', help="Name for the bucket with input data")
    parser.add_argument("-o", "--output_artifact", type=str, help="Name for the output serialized model (Inference Artifact folder)" ,required=True,)
    parser.add_argument('-u', '--mlflow_tracking_uri', type=str, help='mlflow_tracking_uri')
    parser.add_argument('-r', '--AWS_DEFAULT_REGION', type=str, help='AWS_DEFAULT_REGION')
    parser.add_argument('-a', '--MLFLOW_ARTIFACT_URI', type=str, help='MLFLOW_ARTIFACT_URI', default ='s3a://mlflow-otus-test')
    parser.add_argument('-e', '--MLFLOW_S3_ENDPOINT_URL', type=str, help='MLFLOW_S3_ENDPOINT_URL')
    parser.add_argument('-k', '--AWS_ACCESS_KEY_ID', type=str, help='S3 AWS_ACCESS_KEY_ID')
    parser.add_argument('-s', '--AWS_SECRET_ACCESS_KEY', type=str, help='S3 AWS_SECRET_ACCESS_KEY')
    args = parser.parse_args()

    main(args)