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
    # TODO 以SQL风格进行处理
    rdd = sc.textFile("../datas/wordcount/data.txt"). \
        filter(lambda line: line.strip()). \
        flatMap(lambda x: x.split(" ")). \
        map(lambda x: [x])

    df = rdd.toDF(["word"])

    df.createOrReplaceTempView("words")
    spark.sql("select word , count(1) from words group by 1 order by 2 desc").show()

    # TODO DSL风格
    df = spark.read.format("text").load("../datas/wordcount/data.txt")

    # withColumn
    # explode 爆炸函数 可以将['a','b']  拆分
    df2 = df.withColumn("value", F.explode(F.split(df["value"], " ")))
    df2.groupby("value").count().withColumnsRenamed({'value': 'name', 'count': 'cnt'})
