from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

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

    # 基于pandas的Dataframe构建sparksql的dataframe
    pdf = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["fengjing", "huangquan", "baie"],
            "age": [20, 30, 40]
        }
    )

    df = spark.createDataFrame(pdf)
    df.printSchema()
    df.show()
