import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from spark_utils import get_spark_session


@udf(returnType=StringType())
def pretty_str_udf(val):
    return pretty_str(val)


def pretty_str(val):
    return f"*{val}*"


spark = get_spark_session()
spark.udf.register("pretty_str1", pretty_str)
spark.udf.register("pretty_str2", lambda val: pretty_str(val), "string")

data = [(1, "Alex", round(time.time() * 1000) + 1000),
        (2, "Anna", round(time.time() * 1000) + 2000),
        (2, "Anna", round(time.time() * 1000) + 3000),
        (2, "Anna", round(time.time() * 1000) + 4000),
        (4, "Moon", round(time.time() * 1000) + 5000)]

columns = ["id", "name", "ts"]
df = spark.createDataFrame(data=data, schema=columns)

df.createOrReplaceTempView("test")
df.printSchema()

res = spark.sql("""
    select rn, id, name, pretty_str1(name) as name1, pretty_str2(name) as name2,
        ts, from_unixtime(ts / 1000) as ts_date  
    from (
        select id, name, ts,
            row_number() over (partition by id order by ts) as rn
        from test) t2
        where rn = 1
""")

#res.repartition(2).write.mode("overwrite").parquet("/tmp/test")

res.show(truncate=False)
