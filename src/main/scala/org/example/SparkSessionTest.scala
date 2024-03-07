package org.example

import org.apache.spark.sql.SparkSession

object SparkSessionTest {
  def initSpark(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .config("spark.sql.broadcastTimeout", "1200")
      .config("spark.hadoop.hive.exec.dynamic.partition", "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.hadoop.hive.exec.max.dynamic.partitions", "4000")
      .config("spark.sql.storeAssignmentPolicy", "legacy")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.metastore.uris", "thrift://hadoop73:9083")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = initSpark()
    val sql = "select * from sgami_bjzx_gate.a_gate_archive_new limit 2"
    val df = spark.sql(sql)
    df.show()
  }

}