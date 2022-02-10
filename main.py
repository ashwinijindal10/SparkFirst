from pyspark import SparkContext
import collections
from common.logger import Log4j
from lib.config import *
from common.utils import *

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
    db_properties = get_db_config()
    spark.sql("CREATE DATABASE IF NOT EXISTS STAGING_DB ")
    spark.catalog.setCurrentDatabase("STAGING_DB")
    #emailId,phoneNo,password
    _select_sql = "(select * from users) as userinfo"
    df_select = spark.read.jdbc(url=db_properties["url"], properties=db_properties, table=_select_sql)

    df_select.write.mode("overwrite").saveAsTable("user_data")

    logger.info(spark.catalog.listTables("STAGING_DB"))

    df_select.createOrReplaceTempView("temp_users")
    result = spark.sql("select emailId, password from temp_users")

    result.write.format("mongo").mode("overwrite") \
        .option("database", "myFirstDatabase") \
        .option("collection", "test") \
        .save()


if __name__ == '__main__':
    init()
    testSpark()
    #p#rint(get_decodes_string("this is <Encrypted MTIzNDUx> and this is not <Encrypted MTIzNDUx>",decode_pattern))