
def setupSpark():
    config = SparkConf().setMaster(None).setAppName("test")
    sc = SparkContext(config)
    lines = sc.textFile(file_path)
    ratings = lines.map(lambda x: x.split()[2])
    result = ratings.countByValue()
    sortedResult = collections.OrderedDict(result.items())
    for key, value in sortedResult.items():
        print(f"{key} : {value}")



def testSpark():
    result_df = spark.read.text(file_path)
    logger.info("spark started")
    result_df.show()
    spark.stop()
