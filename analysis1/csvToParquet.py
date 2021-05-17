import os

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("PositivePercentage", StringType(), True),
        StructField("NumberofPeoplePositive", StringType(), True),
        StructField("Ratio", StringType(), True)])

    cwd = os.getcwd()
    rdd = sc.textFile(cwd+"/covid19infectionsurveydatasets20210514engfinal.csv").map(lambda line: line.split(","))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet(cwd+'/input-parquet')