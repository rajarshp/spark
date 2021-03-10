from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StringType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7 pyspark-shell'

# create sparksession

sparkSession = SparkSession.builder.master("local").appName("cardanalysis").config(conf=SparkConf()).getOrCreate()
sc = sparkSession.sparkContext
sc.setLogLevel("ERROR")
streamingContext = StreamingContext(sc, 10)


# read local file to create DF and then cast the columns accordingly

columns = ['CustomerName', 'DOB', 'SSN', 'City', 'State', 'ZipCode', 'ExistingProducts', 'OtherBankProducts1',
           'CreditSpent1', 'OtherBankProducts2', 'CreditSpent2', 'OtherBankProducts3', 'CreditSpent3',
           'DefaulterFlag']

customerRefData = sc.textFile("CustomerRefererenceData.csv") \
    .map(lambda a: a.split(",")) \
    .toDF(columns)
print("Check data from loaded file")
print(customerRefData.show(2))

print("Before Cast of the columns")
customerRefData.printSchema()

# cast the required columns to corresponding data type

customerRefData = customerRefData.withColumn("ZipCode", customerRefData["ZipCode"].cast(LongType()))
customerRefData = customerRefData.withColumn("CreditSpent1", customerRefData["CreditSpent1"].cast(LongType()))
customerRefData = customerRefData.withColumn("CreditSpent2", customerRefData["CreditSpent2"].cast(LongType()))
customerRefData = customerRefData.withColumn("CreditSpent3", customerRefData["CreditSpent3"].cast(LongType()))

# check the schema
print("After Cast of the columns")
customerRefData.printSchema()

# register DF for spark sql operation
print("pyspark sql")
customerRefData.createOrReplaceTempView("customerref")

report = sparkSession.sql("select * from customerref where SSN='SSN0001'")

report.show()

################################## CREATE WITH PREDEFINED SCHEMA ##############################################
print("Read file with schema and create DF")
schema = StructType() \
    .add("CustomerName", StringType(), True) \
    .add("DOB", StringType(), True) \
    .add("SSN", StringType(), True) \
    .add("City", StringType(), True) \
    .add("State", StringType(), True) \
    .add("ZipCode", LongType(), True) \
    .add("ExistingProducts", StringType(), True) \
    .add("OtherBankProducts1", StringType(), True) \
    .add("CreditSpent1", LongType(), True) \
    .add("OtherBankProducts2", StringType(), True) \
    .add("CreditSpent2", LongType(), True) \
    .add("OtherBankProducts3", StringType(), True) \
    .add("CreditSpent3", LongType(), True) \
    .add("DefaulterFlag", StringType(), True)

# read the file

df_with_schema = sparkSession.read.format("csv") \
    .option("header", False) \
    .schema(schema) \
    .load("CustomerRefererenceData.csv")

# check the data
print(df_with_schema.select("CustomerName").take(2))
print(df_with_schema.take(2))

df_with_schema.createOrReplaceTempView("newcustomerref")

sparkSession.sql("select * from newcustomerref where SSN='SSN0002'").show()
################################################################################################################

# Start pyspark Streaming

kafkaStream = KafkaUtils.createDirectStream(streamingContext, ["carddata"], {"metadata.broker.list": "localhost:9092"})

lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

streamingContext.start()
streamingContext.awaitTermination()

