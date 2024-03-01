package org.example

import org.apache.spark.sql.SparkSession


object SparkSessionTest extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples.com")
    .getOrCreate()

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  // Spark Create DataFrame from RDD
  val rdd = spark.sparkContext.parallelize(data)

  // Create DataFrame
  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.show()


}