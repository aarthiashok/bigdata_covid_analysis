from pyspark.shell import spark

parquetFile = spark.read.parquet('/home/aarthi/spark/covid-analysis/input-parquet')
parquetFile.createOrReplaceTempView("parquetFile")

covidPositive = spark.sql("SELECT Date,NumberofPeoplePositive FROM parquetFile")
covidPositive.show()


