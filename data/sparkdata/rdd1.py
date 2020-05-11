# load log files from local filesystem
wordrdd = sc.textFile("word.txt")
wordrdd.take(1)
wordrdd.count()
wordrdd.getNumPartitions()
# filter log records for spark only
sparkrdd = wordrdd.filter(lambda line: "Spark" in line)
# save sparkrdd as a file
sparkrdd.saveAsTextFile("./Temp/sparkrdd.txt")