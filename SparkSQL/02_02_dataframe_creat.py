from pyspark.sql import SparkSession
import os

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

    # 构造DataFrame对象
    # 参数1 被转换的rdd 指定列名按照顺序依次传入
    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    # 打印表结构信息
    df.printSchema()

    # 打印表信息
    # 参数1 打印多少数据 参数2 表示数据列过长时时候隐藏
    df.show(20, False)

    # 将df转换为注册临时表
    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age < 30").show()
