from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

"""
需求1: 各个省的销售额的统计
需求2: TOP3销售省份中，有多少店铺到达销售额前1000
需求3: TOP3各省份中，各个省份的平均单价
需求4: TOP3各个省份中，支付比例

receivable-订单金额
storeProvince-省份
dateTS-销售日期
payType-支付类型
storeID-店铺id
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:/evm/java'
    # 配置hadoop路径
    os.environ['HADOOP_HOME'] = 'D:/evm/hadoop-2.7.1'
    # 配置base环境Python解释器路径
    os.environ['PYSPARK_PYTHON'] = 'D:/evm/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/evm/miniconda3/python.exe'

    # 构建SparkSession执行环境入口变量
    spark = SparkSession.builder. \
        appName("Spark_project"). \
        config("spark.sql.shuffle.partitions", "2"). \
        master("local[*]"). \
        getOrCreate()

    sc = spark.sparkContext

    # 1拿到数据，并对缺失值进行过滤，过滤省份信息为空的数据
    # 2订单金额，订单单笔金额超过1W的是测试数据，需要过滤
    # 3需要进行进行列值裁剪，性能优化
    df = spark.read.format("json").load("../datas/mini.json"). \
        dropna(thresh=1, subset=['storeProvince']). \
        filter("storeProvince != 'null'"). \
        filter("receivable < 10000"). \
        select("receivable", "storeProvince", "dateTS", "payType", "storeID")

    # TODO 各个省的销售额的统计
    province_sale_df = (
        df.groupBy("storeProvince")
        .agg(F.sum("receivable").alias("money"))
        .withColumn("money", F.round("money", 2))
        .orderBy(F.col("money").desc())
    )

    province_sale_df.show(truncate=False)

    # 写出到Mysql
    # province_sale_df.write.mode("overwrite"). \
    #     format("jdbc"). \
    #     option("url", "jdbc:mysql://127.0.0.1:3306/pythondb?useSSL=false&Unicode=true&characterEncoding=utf-8"). \
    #     option("dbtable", "province_sale"). \
    #     option("user", "root"). \
    #     option("password", "123456"). \
    #     option("encoding", "utf-8"). \
    #     save()

    print("province_sale_d===》写入成功")

    # TODO TOP3销售省份中，有多少店铺到达销售额前1000
    top3_province = province_sale_df.limit(3).select("storeProvince")

    top3_province_df_join = df.join(top3_province, on="storeProvince")
    top3_province_df_join.show(truncate=False)

    top3_province_df_result = top3_province_df_join.groupBy(
        ['storeProvince', 'storeID', F.unix_timestamp(df['dateTS'].substr(0, 10), "yyyy-MM-dd").alias("day")]). \
        sum('receivable').withColumnRenamed("sum(receivable)", "money"). \
        filter("money > 1000"). \
        drop_duplicates(subset=["storeID"]). \
        groupBy("storeProvince").count()

    top3_province_df_result.show()
