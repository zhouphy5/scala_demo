package org.example

import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream}
import java.util
import scala.collection.JavaConversions._

object SparkSessionTest {
  def initSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo")
      .config("spark.sql.broadcastTimeout", "1200")
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.hadoop.hive.exec.max.dynamic.partitions", "4000")
      .config("spark.sql.storeAssignmentPolicy", "legacy")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def load_config(path: String): util.Map[String, Any] = {
    val ios = new FileInputStream(new File(path))
    val yaml = new Yaml()
    val config = yaml.loadAs(ios, classOf[util.Map[String, Any]]).toMap
    config
  }


  def main(args: Array[String]): Unit = {
    //    val config = load_config("/root/Projects/scala_demo/src/main/scala/org/example/data_trans_config.yaml")
    val spark = initSpark()
    //    val schema = Seq("department", "emp_count")
    //    val data = Seq(("engineering", 500), ("accounts", 100), ("sales", 1500),
    //      ("marketing", 500), ("finance", 150))
    //    val rdd = spark.sparkContext.parallelize(data)
    //    val df = rdd.toDF(schema: _*)
    //    df.show()
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092")
      .option("subscribe", "test-topic")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 1)
      .option("group.id", "2")
      .option("auto.offset.reset", "earliest")
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()

  }

}
