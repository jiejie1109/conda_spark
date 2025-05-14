from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口变量
    spark = SparkSession.builder. \
        appName("text"). \
        master("local[*]"). \
        getOrCreate()

    # 通过SparkSession获取SparkCntext对象
    sc = spark.sparkContext

    # SparkSql的入口
    df = spark.read.csv("../datas/sougou/stu_score.txt", sep=",", header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show()
    # 创建表名
    df2.createTempView("score")

    # Sql写法
    spark.sql("""
        select * from score where name ="语文" LIMIT 10 
    """).show()

    # DSL写法
    df2.where("name='语文'").limit(5).show()
