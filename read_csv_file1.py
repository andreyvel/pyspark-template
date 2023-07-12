from pyspark.sql.functions import col, expr
from spark_utils import get_spark_session

spark = get_spark_session()

# read csv without schema
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .load("./data/btcusdt_1m_kline.csv")

df.printSchema()

df = df.withColumn("date", col("<date>").cast("int")) \
    .withColumn("time", col("<time>").cast("int")) \
    .withColumn("open", col("<open>").cast("double")) \
    .withColumn("low", col("<low>").cast("double")) \
    .withColumn("high", col("<high>").cast("double")) \
    .withColumn("close", col("<close>").cast("double")) \
    .drop("<date>", "<time>", "<open>", "<low>", "<high>", "<close>", "<vol>")

df.createOrReplaceTempView("btcusdt")
df.printSchema()

spark.sql("""
    select * from btcusdt
    limit 10
""").show()
