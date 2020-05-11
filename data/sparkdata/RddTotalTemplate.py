wordrdd = sc.textFile("word.txt")
sparkrdd = wordrdd.filter(lambda line: "Spark" in line)

lines = sc.textFile("word.txt")
words=lines.map(lambda line : line.split(" "))

lines = sc.textFile("word.txt")
words=lines.flatMap(lambda line : line.split(" "))

lines = sc.textFile("word.txt")
words=lines.flatMap(lambda line : line.split(" "))
word1=words.map(lambda word: (word,1))
wordgroupbykey=word1.groupByKey()
temp=wordgroupbykey.map(lambda x: (x[0],list(x[1])))


lines = sc.textFile("word.txt")
words=lines.flatMap(lambda line : line.split(" "))
word1=words.map(lambda word: (word,1))
wordcount=word1.reduceByKey(lambda x,y : x+y)

lines = sc.textFile("word.txt")
words=lines.flatMap(lambda line : line.split(" "))
word1=words.map(lambda word: (word,1))
wordgroupbykey=word1.groupByKey()
temp=wordgroupbykey.map(lambda x: (x[0],list(x[1])))

kvpairs = sc.parallelize([('city', 'Hayward')
, ('state', 'CA')
, ('zip', 94541)
, ('country', 'USA')])
kvpairs.keys().collect()

kvpairs = sc.parallelize([('city','Hayward')
,('state','CA')
,('zip',94541)
,('country','USA')])
kvpairs.values().collect()

kvpairs = sc.parallelize([('city','Hayward')
,('state','CA')
,('zip',94541)
,('country','USA')])
kvpairs.sortByKey()

readme = sc.textFile('word.txt')
words = readme.flatMap(lambda x: x.split(' '))
sortbyfirstletter = words.sortBy(lambda x: x[0].lower(),
ascending=False)
sortbyfirstletter.take(5)

locwtemps = sc.parallelize(['Hayward,71|69|71|71|72',
'Baumholder,46|42|40|37|39',
'Alexandria,50|48|51|53|44',
'Melbourne,88|101|85|77|74'])
kvpairs = locwtemps.map(lambda x: x.split(','))
kvpairs.take(4)
locwtemplist = kvpairs.mapValues(lambda x: x.split('|')) \
.mapValues(lambda x: [int(s) for s in x])
locwtemplist.take(4)
locwtemps = kvpairs.flatMapValues(lambda x: x.split('|')) \
.map(lambda x: (x[0], int(x[1])))
locwtemps.take(3)

locations = sc.parallelize([('Hayward', 'USA', 1)
,('Baumholder','Germany', 2)
,('Alexandria','USA', 3)
,('Melbourne','Australia', 4)])
bylocno = locations.keyBy(lambda x: x[2])
bylocno.collect()
# returns:
#[(1, ('Hayward', 'USA', 1)), (2, ('Baumholder', 'Germany', 2)),
# (3, ('Alexandria', 'USA', 3)), (4, ('Melbourne', 'Australia', 4))]

stores = sc.parallelize([(100, 'Boca Raton'),
(101, 'Columbia'),
(102, 'Cambridge'),
(103, 'Naperville')])
# stores schema (store_id, store_location)
salespeople = sc.parallelize([(1, 'Henry', 100),
(2, 'Karen', 100),
(3, 'Paul', 101),
(4, 'Jimmy', 102),
(5, 'Janice', None)])
# salespeople schema (salesperson_id, salesperson_name, store_id)
salespeople.keyBy(lambda x: x[2]) \
.join(stores).collect()
# returns: [(100, ((1, 'Henry', 100), 'Boca Raton')),
# (100, ((2, 'Karen', 100), 'Boca Raton')),
# (102, ((4, 'Jimmy', 102), 'Cambridge')),
# (101, ((3, 'Paul', 101), 'Columbia'))]

people_json_file = 'people.json'
people_df = spark.read.json(people_json_file)
people_df.show()


