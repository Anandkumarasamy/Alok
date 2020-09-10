from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

sparksession=SparkSession.builder.appName('Data frame with own schema').getOrCreate()

data_schema=[StructField('Serial_No',IntegerType(),True),
            StructField('Area_Name',StringType(),True),
            StructField('Rate',IntegerType(),True),
            StructField('BHK',StringType(),True),
            StructField('No_of_Bathrooms',StringType(),True),
            StructField('Sq-feet',IntegerType(),True),
            StructField('Rate/Sq-feet',IntegerType(),True),
            StructField('Status',StringType(),True)]
final_stru=StructType(fields=data_schema)
df=sparksession.read.csv('/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv'
,final_stru,header=True)

df.show()

df.filter(df.Area_Name.like("S%")).show()

df.groupBy('Area_Name').count().show()

df.groupBy('Area_Name').count().filter('count > 20').show()

df.orderBy('Area_Name').show()