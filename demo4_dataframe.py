from pyspark.sql import SparkSession

sparksession = SparkSession.builder.appName("Data_Frame_sql example").getOrCreate()

df=sparksession.read.csv('/home/anand/Desktop/PYSPARK_Program/git_example/python-spark-tutorial/in/RealEstate.csv',
mode='DROPMALFORMED',inferSchema=True,header=True)

df.createOrReplaceTempView('product')

sql=sparksession.sql('select * from product')

sql.show()