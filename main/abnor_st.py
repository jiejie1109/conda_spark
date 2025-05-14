import os
import re
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

    conf = SparkConf().setMaster("local[*]").setAppName("广播变量与累加器练习")
    sc = SparkContext(conf=conf)

    acc_rdd = sc.textFile("../datas/sougou/accumulator_broadcast_data.txt")

    # 特殊字符list
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 将特殊变量转变为广播器
    broadcast = sc.broadcast(abnormal_char)

    # 对特殊字符进行累加
    acmult = sc.accumulator(0)

    # 处理数据的空行
    lines_rdd = acc_rdd.filter(lambda line: line.strip())

    # 去除前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 对数据进行进行切分，用正则表达式进行分割 \s+表示不确定多少空格
    words_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))

    # 当前rdd中比较复杂
    def filter_func(data):
        global acmult
        # 去除特殊符号的定义
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmult += 1
            return False
        else:
            return True


    normal_words_rdd = words_rdd.filter(filter_func)
    result_rdd = normal_words_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    print(result_rdd.collect(), acmult)
