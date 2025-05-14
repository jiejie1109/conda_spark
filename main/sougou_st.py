import jieba
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

    conf = SparkConf().setMaster("local[2]").setAppName("rdd的创建方式")
    sc = SparkContext(conf=conf)

    # 读取数据文件
    file_rdd = sc.textFile("../datas/sougou/SogouQ.txt")

    # 对数据进行切分
    split_rdd = file_rdd.map(lambda x: x.split('\t'))

    # split数据会进行多次复用
    """
    MEMORY_ONLY	仅内存（默认方式，速度快，但内存不足时会重新计算）
    MEMORY_AND_DISK	优先内存，内存不足时存磁盘（推荐）
    DISK_ONLY	仅磁盘（速度慢，但节省内存）
    MEMORY_ONLY_SER	内存 + 序列化（节省空间，但增加 CPU 开销）
    MEMORY_AND_DISK_SER	内存 + 磁盘 + 序列化（平衡方案）
    """
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO 需求1：分析热点词语
    # withReplacement: bool 允许重复采样
    # print(split_rdd.takeSample(True, 3))

    # 取出搜索字段
    context_rdd = split_rdd.map(lambda x: x[2])

    # 转变搜索关键词
    word_rdd = context_rdd.flatMap(context_jieba)

    # print(word_rdd.collect())
    filter_rdd = word_rdd.filter(filter_word)
    # 将关键词转换回来
    final_rdd = filter_rdd.map(append_word)
    # 对单词进行分组统计分析
    result1 = final_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)

    print("需求结果1：", result1)

    # TODO 需求2：用户和关键词组合分析
    uer_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对结果再次组合
    user_word_rdd_withone = uer_rdd.flatMap(extract_user_world)
    # 聚合
    result2 = user_word_rdd_withone.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                           numPartitions=1).take(5)

    print("需求结果2：", result2)

    # TODO 需求3：热门时间段分析
    time_rdd = split_rdd.map(lambda x: x[0])
    # 对时间进行处理 取以冒号分割，并取第一位小时，并返回为元组
    hour_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    # add 来自方法 两数相加
    result3 = hour_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print("需求结果3", result3)
