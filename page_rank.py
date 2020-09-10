from pyspark import SparkConf , SparkContext

conf=SparkConf().setAppName('Page_Rank').setMaster('local[2]')
sc=SparkContext(conf=conf)

lines=sc.parallelize([("A B"), ("A C"), ("A D"), ("B A"), ("B C"), ("C D"), ("E B")])

links=lines.flatMap(lambda x : x.split(',')).map(lambda x :(x[0],x[2])).groupByKey().mapValues(dict)
#[('A', ['B', 'C', 'D']), ('C', ['D']), ('E', ['B']), ('B', ['A', 'C'])]

ranks=links.mapValues(lambda y : 1.0)
#[('A', 1.0), ('C', 1.0), ('E', 1.0), ('B', 1.0)]

links_ranks=links.join(ranks).values()
#[(['B', 'C', 'D'], 1.0), (['D'], 1.0), (['B'], 1.0), (['A', 'C'], 1.0)]

links_count=links_ranks.map(lambda x : (x[0],(x[1]/(len(x[0])))))
#[(['B', 'C', 'D'], 0.3333333333333333), (['D'], 1.0), (['B'], 1.0), (['A', 'C'], 0.5)]

overall_rank=links_count.map(lambda x : (x[0],("%.4f" %((x[1]*0.85)+0.15))))
#[(['B', 'C', 'D'], '0.4333'), (['D'], '1.0000'), (['B'], '1.0000'), (['A', 'C'], '0.5750')]

result=overall_rank.map(lambda x: x.split(','))
print(result.collect())
