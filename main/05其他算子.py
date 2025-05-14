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

    rdd_kv = sc.parallelize([('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)], numSlices=3)
    # 打印key
    rdd_kv.keys().foreach(lambda x: print(x))
    # 打印value
    rdd_kv.values().foreach(lambda x: print(x))
    # 对value进行操作
    rdd_kv.mapValues(lambda value: value + 1).foreach(lambda x: print(x))

    print(rdd_kv.collectAsMap())

    sc.stop()
