from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FirstPySpark").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# read file

data = sc.textFile("pokemon.csv")

# extract header

header = data.first()
print("File Header")
print(header)

# remove the header

pokemon = data.filter(lambda line: line != header).map(lambda l: l)

# check the data
print("File Data")
print(pokemon.take(2))

# find max defence score

maxDef = pokemon.map(lambda r: r.split(",")).map(lambda r: int(r[7])).max()

print("max pokemon defence score: ", maxDef)

# find top 5 defence score

max5Def = pokemon.map(lambda r: r.split(",")).map(lambda r: int(r[7])).top(5)
print("Top 5 Defence score ", max5Def)

# find top 5 distinct defence score

max5UniqDef = pokemon.map(lambda r: r.split(",")).map(lambda r: int(r[7])).distinct().top(5)
print("Top 5 Defence score ", max5UniqDef)
