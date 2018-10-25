#Code here

from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").load("/home/ubuntu/eclipse-workspace/exam1-sp17-bigdata-desc/hive.studentsPR.csv")

df.createOrReplaceTempView("estudiantes")

df2 = spark.read.format("csv").load("/home/ubuntu/eclipse-workspace/exam1-sp17-bigdata-desc/hive.escuelasPR.csv")

df2.createOrReplaceTempView("escuelas")

spark.sql("select * from estudiantes natural join escuelas using(school_id) where (ciudad='Ponce' or ciudad='San Juan') and escuelas.nivel='Superior').show()
