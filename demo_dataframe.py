from pyspark.sql import SparkSession

sparksession=SparkSession.builder.appName('Dataframe example').getOrCreate()

df=sparksession.read.csv('/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv',
mode='DROPMALFORMED',inferSchema='True',header='True')

#/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv

'''lis=[('Praveen', 1, 1000),('nireekshan', 2, 2000),('Ramesh', 3, 3000)]

sparksession.createDataFrame(lis,['Name','No Of Iteams','Price']).show()'''

print(df.columns)
df.show(5)
print(df.filter("Bedrooms >= 3 and price >545000").select('Location','Size','Status').distinct().count())