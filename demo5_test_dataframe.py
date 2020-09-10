from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

sparksession=SparkSession.builder.appName('Testing').getOrCreate()

lis =[('praveen',50000,27),('Mani',45000,24),('Sanjay',30000,26)]

sparksession.createDataFrame(lis,['Name','Salary','Age']).show()

df=sparksession.read.csv('/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv'
,mode='DROPMALFORMED',inferSchema=True,header=True)

df.show()

df.filter('Price > 500000 and Bedrooms > 3').select('Location','Status').distinct().show()

print(df.groupBy('Location').count().show())

df.orderBy('Bedroom').select('Location','Bedroom').show()

data_schema=[StructField('Serial_No',IntegerType(),True),
            StructField('Area_Name',StringType(),True),
            StructField('Rate',IntegerType(),True),
            StructField('BHK',StringType(),True),
            StructField('No_of_Bathrooms',StringType(),True),
            StructField('Sq-feet',IntegerType(),True),
            StructField('Rate/Sq-feet',IntegerType(),True),
            StructField('Status',StringType(),True)]
final_stru=StructType(fields=data_schema)
df1=sparksession.read.csv('/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv'
,final_stru,header=True)

df1.filter(df.Area_Name.like("S%")).show()

df1.groupBy('Area_Name').count().show()

df1.groupBy('Area_Name').count().filter('count > 20').show()

df1.orderBy('Area_Name').show()