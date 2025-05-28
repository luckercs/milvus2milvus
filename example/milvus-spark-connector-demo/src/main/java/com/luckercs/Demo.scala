package com.luckercs

import org.apache.spark.sql.SparkSession


object Demo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("milvusSparkConnectorDemo").master("local[*]").getOrCreate()

    val df = spark.read.format("milvus")
      .option("uri", MilvusInstance.uri)
      .option("collection", MilvusInstance.COLLECTION_NAME)
      .load()

    df.show(5, false)
    println(df.count())
    spark.close()
  }
}
