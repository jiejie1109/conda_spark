import math
import os
import re

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:/evm/java'
    # 配置hadoop路径
    os.environ['HADOOP_HOME'] = 'D:/evm/hadoop-2.7.1'
    # 配置base环境Python解释器路径
    os.environ['PYSPARK_PYTHON'] = 'D:/evm/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/evm/miniconda3/python.exe'

    conf = SparkConf().setMaster("local[2]").setAppName("rdd的创建方式")
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 6, 3, 4, 5, 7, 8, 9, 10])
    # reduce 聚合 x相当于一个临时变量
    print(rdd1.reduce(lambda x, y: x + y))
    # 打印最大的三个数
    print(rdd1.top(3))
    # 打印最小的三个数
    print(rdd1.takeOrdered(3))
    sc.stop()
