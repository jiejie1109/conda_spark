from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:/evm/java'
    # 配置hadoop路径
    os.environ['HADOOP_HOME'] = 'D:/evm/hadoop-2.7.1'
    # 配置base环境Python解释器路径
    os.environ['PYSPARK_PYTHON'] = 'D:/evm/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/evm/miniconda3/python.exe'

    # 构建SparkSession执行环境入口变量
    spark = SparkSession.builder. \
        appName("text"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext
    schema = StructType().add("user_id", StringType(), nullable=True). \
        add("movie", IntegerType(), nullable=True). \
        add("rank", IntegerType(), nullable=True). \
        add("ts", StringType(), nullable=True)

    df = spark.read.format("csv"). \
        option("sep", "\t"). \
        option("header", False). \
        option("encoding", "utf-8"). \
        schema(schema=schema). \
        load("../datas/wordcount/u.data")

    # TODO 用户平均分
    (df.groupBy("user_id")
     .agg(F.avg("rank").alias("avg_rank"))  # 计算平均值并命名为avg_rank
     .withColumn("avg_rank", F.round("avg_rank", 2))  # 四舍五入保留2位小数
     .orderBy(F.col("avg_rank").desc())  # 按avg_rank降序排列
     .show())

    # TODO 电影的平均分处理
    (
        df.groupBy("movie")
        .agg(F.avg("rank").alias("movie_avg_rank"))
        .withColumn("movie_avg_rank", F.round("movie_avg_rank", 2))
        .orderBy(F.col("movie_avg_rank").desc())
        .show()
    )

    # TODO 查询大于平均分的电影的数量
    (
        df.where(

        )
    )
