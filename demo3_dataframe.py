from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Frame example").getOrCreate()

l = [('Praveen', 1, 1000), ('nireekshan', 2, 2000), ('Ramesh', 3, 3000)]

spark.createDataFrame(l, ['name', 'age', 'salary']).show()