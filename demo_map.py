from pyspark import SparkContext

sc1=SparkContext('local[2]','Map_Narrow transformation_Example1-1')

sc1.stop()

sc2=SparkContext('local[2]','Map_Narrow transformation_Example1-2')

items_cost = [1000, 2000, 3000, 3000,2000,10000,7000,1000,5000,6000,900.8000]

items_cost_rrd=sc2.parallelize(items_cost)

items_cost_with_gst=items_cost_rrd.map(lambda X: X+50).collect()

print('Iteam cost are:',items_cost)

print('iteam cost with gst:',list(items_cost_with_gst))