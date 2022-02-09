from pyspark import SparkContext
import collections
from lib.logger import Log4j
from lib.config import *

from pyspark.sql import SparkSession

file_path = "D:\\Project\\Apache_Spark\\data\\ml-100k\\u.data"
spark: SparkSession = None
logger: Log4j = None


def init():
    global spark, logger

    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)


def testSpark():
    result_df = spark.read.text(file_path)
    logger.info("spark started")
    result_df.show()
    spark.stop()


def setupSpark():
    config = SparkConf().setMaster(None).setAppName("test")
    sc = SparkContext(config)
    lines = sc.textFile(file_path)
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()
    sortedResult = collections.OrderedDict(result.items())
    for key, value in sortedResult.items():
        print(f"{key} : {value}")


def testSpark2():
    db_properties = get_db_config()
    _select_sql = "(select * from users) as users"
    df_select = spark.read.jdbc(url=db_properties["url"],  properties=db_properties, table =_select_sql)
    df_select.write.format("mongo").mode("append") \
        .option("database", "myFirstDatabase") \
        .option("collection", "test") \
        .save()


if __name__ == '__main__':
    init()
    testSpark2()
