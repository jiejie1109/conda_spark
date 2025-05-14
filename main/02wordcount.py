import os
from pyspark import SparkConf, SparkContext

str = ["""hadoop spark
hive hadoop spark spark
hue hbase hbase hue hue

hadoop spark
hive hadoop spark spark
hue hbase hbase hue hue

hadoop spark
hive hadoop spark spark
hue hbase hue hue
hadoop spark"""]


# 使用函数代替 lambda 表达式
def non_empty_lines(line):
    return len(line) > 0


def split_lines(line):
    return line.strip().split()


def word_to_pair(word):
    return (word, 1)


# 定义函数，用于打印每个元素
def print_element(element):
    print(element)


if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:/evm/java'
    # 配置hadoop路径
    os.environ['HADOOP_HOME'] = 'D:/evm/hadoop-2.7.1'
    # 配置base环境Python解释器路径
    os.environ['PYSPARK_PYTHON'] = 'D:/evm/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/evm/miniconda3/python.exe'

    conf = SparkConf().setMaster("local[2]").setAppName("第一个Spark程序")
    sc = SparkContext(conf=conf)
    fileRdd = sc.textFile("../datas/wordcount/data.txt")
    rsRdd = fileRdd.filter(lambda x: len(x) > 0).flatMap(lambda line: line.strip().split(" ")) \
        .map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    rsRdd.saveAsTextFile("../datas/wordcount/result")

    sc.stop()
