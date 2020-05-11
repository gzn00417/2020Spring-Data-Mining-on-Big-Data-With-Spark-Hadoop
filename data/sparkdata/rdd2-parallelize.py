parallelrdd = sc.parallelize([1, 2, 3, 4, 5])
parallelrdd
# notice the type of RDD created:
# ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:423
parallelrdd.count()
parallelrdd.collect()
# will return the parallel collection as a list as follows:
