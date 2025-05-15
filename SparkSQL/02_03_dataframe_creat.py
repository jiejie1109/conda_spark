from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StringType, IntegerType

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
    # 基于RDD转化为为Dataframe

    rdd = sc.textFile("../datas/sougou/people.txt"). \
        map(lambda x: x.split(",")). \
        map(lambda x: (x[0], int(x[1])))

    # toDF的方式
    df1 = rdd.toDF(['name', 'age'])
    df1.printSchema()
    df1.show()

    # toDF的方式2
    schema = StructType().add('name', StringType(), nullable=True). \
        add('age', IntegerType(), nullable=False)

    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()
