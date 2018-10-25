#Code here

# this code follows the code implemented on p3a.py

spark.sql("select distrito, ciudad, count(*) from escuelas where region='Arecibo').show()
