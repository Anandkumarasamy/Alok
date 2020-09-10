from pyspark.sql import SparkSession

sparksession=SparkSession.builder.appName('Second Program').getOrCreate()

print('Spark Session object has been created Successfully')