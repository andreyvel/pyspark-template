from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

from spark_utils import get_spark_session

spark = get_spark_session()

schema = StructType([
    StructField("date", IntegerType(), True),
    StructField("time", IntegerType(), True),
    StructField("open", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("vol", DoubleType(), True)
])

df = spark.read.schema(schema).format("csv") \
    .option("delimiter", ",") \
    .load("./data/btcusdt_1m_kline.csv")

df = df.withColumn("year", expr("cast(date / 10000 as int) as year"))
df.createOrReplaceTempView("btcusdt")
df.printSchema()

spark.sql("""
    select year, count(1) as rows,
        min(low) as min_price, avg(close) as avg_price, max(high) as max_price 
    from btcusdt
    where year is not null
    group by year 
""").show(10)
