# 累加器
import os
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from main.sougou_def import context_jieba, filter_word, append_word, extract_user_world
from operator import add

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:/evm/java'
    # 配置hadoop路径
    os.environ['HADOOP_HOME'] = 'D:/evm/hadoop-2.7.1'
    # 配置base环境Python解释器路径
    os.environ['PYSPARK_PYTHON'] = 'D:/evm/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/evm/miniconda3/python.exe'

    conf = SparkConf().setMaster("local[*]").setAppName("累加变量")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
    # spark提供的累加器变量，其中参数是初始值
    acmlt = sc.accumulator(0)


    def map_func(data):
        global acmlt
        acmlt += 1
        print(acmlt)


    rdd2 = rdd.map(map_func)
    # rdd2.cache() 先缓存 避免后续调用rdd2 的时候  回溯导致acmlt重复计算
    rdd2.cache()
    rdd2.collect()
    # print(acmlt)

    rdd3 = rdd.map(lambda x: x)
    rdd3.collect()
    print(acmlt)
