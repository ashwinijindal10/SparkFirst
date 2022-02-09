import configparser
from pyspark import SparkConf


def get_config_parser():
    config = configparser.ConfigParser()
    config.read("app.conf")
    return config


def get_spark_app_config():
    spark_conf = SparkConf()
    config = get_config_parser()
 #base64.decodestring()
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_db_config():
    config = get_config_parser()
    db_properties = {}
    for (key, val) in config.items("PG_DB_CONFIGS"):
        db_properties[key] = val
    return db_properties


'''
spark.jars = drivers/postgresql-42.3.2.jar ,drivers/mongo-spark-connector_2.12-3.0.1.jar
spark.driver.extraClassPath =drivers/*
spark.executor.extraClassPath =drivers/*
'''


######$


def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)


def count_by_country(survey_df):
    return survey_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
