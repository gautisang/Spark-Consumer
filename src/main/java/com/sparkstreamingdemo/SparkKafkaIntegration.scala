package com.sparkstreamingdemo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.sql.{ SparkSession, SQLContext }
import org.apache.spark.sql.functions._
object SparkKafkaIntegration extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("DataFrame Basics")
    .config("spark.sql.warehouse.dir", "c:\\tmp\\hive")
    //.enableHiveSupport()
    .getOrCreate()

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092,anotherhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[IntegerDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "group1",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

  val topics = Array("spark-producer-topic")
  val stream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
    .option("subscribe", "spark-producer-topic")
    //.options()
    .option("startingOffsets", "earliest")
    .load()

  val parsed = stream.selectExpr("CAST(value AS STRING)")
  println(parsed.printSchema())

  import spark.implicits._
  import scala.util.matching.Regex
  //   // Extract host name
  val host_pattern = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"
  val ts_pattern = "(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})"
  val method_uri_protocol_pattern = "\\\"(\\S+)\\s(\\S+)\\s*(\\S*)\\\""
  val status_pattern = "\\s(\\d{3})\\s"
  val content_size_pattern = "\\s(\\d+)$"

  val logs_df = parsed.select(
    regexp_extract($"value", host_pattern, 1).alias("host"),
    regexp_extract($"value", ts_pattern, 1).alias("timestamp"),
    regexp_extract($"value", method_uri_protocol_pattern, 1).alias("method"),
    regexp_extract($"value", method_uri_protocol_pattern, 2).alias("endpoint"),
    regexp_extract($"value", method_uri_protocol_pattern, 3).alias("protocol"),
    regexp_extract($"value", status_pattern, 1).alias("status"),
    regexp_extract($"value", content_size_pattern, 1).alias("content_size"))

 // print(logs_df.count())

 val query=logs_df.writeStream
    .format("csv")
    .option("startingOffsets", "earliest")
    .option("path", "../DemoStream/target/Output")
    .option("checkpointLocation", "../DemoStream/target/checkpoint")
    .start()
    
    query.awaitTermination()
}