import time
from pyspark.sql import Window
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from spark_utils import get_spark_session


@udf(returnType=IntegerType())
def squared_udf(val):
    return val * val


spark = get_spark_session()

data = [(1, "Alex", round(time.time() * 1000) + 1000),
        (2, "Anna", round(time.time() * 1000) + 2000),
        (2, "Anna", round(time.time() * 1000) + 3000),
        (2, "Anna", round(time.time() * 1000) + 4000),
        (4, "Moon", round(time.time() * 1000) + 5000)]

columns = ["id", "name", "ts"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()

windowSpec = Window.partitionBy("id").orderBy("ts")

res = df.select(col("id"), squared_udf("id").alias("id2"), col("name"), col("ts"))\
    .withColumn("rn", F.row_number().over(windowSpec))\
    .filter("rn = 1")

res.show(truncate=False)
