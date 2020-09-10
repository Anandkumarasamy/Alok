from pyspark import SparkConf , SparkContext

conf = SparkConf().setAppName('Foreach Example').setMaster('local[2]')
sc = SparkContext(conf=conf)

emp_sal_lis = [1000,8000,10000,50000,800,6000,60000,9000,90000]
withbonus = []

def bonus (n):
    opera = n*1.10
    return opera


empsal_RDD = sc.parallelize(emp_sal_lis)

withbonus.append(empsal_RDD.foreach(lambda x: bonus(x)))

#result = numbersRDD.map(lambda x: div_two(x))

for result in withbonus:
    print('Result are:',result) 
