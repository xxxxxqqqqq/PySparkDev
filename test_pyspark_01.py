from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)
data = range(10)
dist_data = sc.parallelize(data)
print dist_data.reduce(lambda a, b: a+b)
print(sc.version)