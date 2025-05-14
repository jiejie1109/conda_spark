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
    print(sc)
    # 转换算子map  计算每个函数的立方
    # list01 = [1, 2, 3, 4, 5]
    # reRdd = sc.parallelize(list01)
    # reRdd = reRdd.map(lambda x: math.pow(x, 3))
    # # 触发算子 foreach
    # reRdd.foreach(lambda x: print(x))

    # 转换算子 flatMap  切割字符串
    # reRdd = sc.textFile("../datas/wordcount/poem.txt")
    # reRdd = reRdd.flatMap(lambda x: x.strip().split(" "))
    # reRdd.foreach(lambda x: print(x))

    # 过滤算子 filter   filter->要对过滤条件进行判断
    # reRdd = sc.textFile("../datas/wordcount/poem2.txt")
    # reRdd = reRdd.filter(lambda line: len(re.split("\s+", line)) == 4)
    # reRdd.foreach(lambda x: print(x))

    # groupBykey  只分组不聚合
    # list1 = [("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5), ("c", 6)]
    # # ("a",[1,4]), ("b",[2,5]),("c",[3,6])
    # reRdd3 = sc.parallelize(list1, numSlices=3)
    # rdd4 = reRdd3.groupByKey()
    # for key, value in rdd4.collect():
    #     print(key)
    #     for v in value:
    #         print(v)
    #     print("-------------")
    # # reduceByKey  分组聚合

    # sortBy算子  sortByKey算子
    reRdd = sc.textFile("../datas/wordcount/sortr.txt")


    def get_tuple(line):
        arr = re.split(",", line)
        return arr[0], arr[1], arr[2]


    # 将每一行数据切分
    reRdd.map(lambda line: get_tuple(line)).sortBy(lambda x: x[1], ascending=False).foreach(lambda x: print(x))

    sc.stop()
