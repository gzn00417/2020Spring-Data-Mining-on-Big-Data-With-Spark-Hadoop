from pyspark.sql import SparkSession
spark = SparkSession.builder \
.master("spark://sparkmaster:7077") \
.appName("My Spark Application") \
.config("spark.submit.deployMode", "client") \ .getOrCreate()
numlines = spark.sparkContext.textFile("./example.py") \
.count()
print("The total number of lines is " + str(numlines))