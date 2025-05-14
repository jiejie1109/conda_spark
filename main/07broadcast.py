# 广播变量
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

    conf = SparkConf().setMaster("local[2]").setAppName("广播变量")
    sc = SparkContext(conf=conf)

    stu_info_list = [
        (1, '张三', 18),
        (2, '李四', 19),
        (3, '王五', 20),
    ]

    score_info_rdd = sc.parallelize([
        (1, '语文', 100),
        (2, '数学', 90),
        (3, '数学', 90),
        (1, '语文', 100),
        (2, '数学', 90),
        (3, '数学', 90),
        (1, '语文', 100),
        (2, '数学', 90),
        (3, '数学', 90),
        (1, '语文', 100),
        (2, '数学', 90),
        (3, '数学', 90),

    ])

    # 将本地list标记为广播变量
    broadcast_list = sc.broadcast(stu_info_list)
    # 使用广播变量从broadcast_list对象中取出对应的值
    value = broadcast_list.value


    def map_fun(data):
        id = data[0]
        name = ""
        for stu_info in value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]
        return name, data[1], data[2]


    print(score_info_rdd.map(map_fun).collect())
"""
运用场景：本地集合对象 和 分布式集合对象 进行关联的时候
可以将本地集合对象转变为广播变量
"""