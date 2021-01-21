package top_tweets

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, dayofmonth, from_utc_timestamp, month, to_utc_timestamp, year}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/** class used to instantiate Kafka Consumer  */
class TopTweetsConsumer extends Settings {

  // setting configuration parameters for runConsumer function
  val brokers: String = setBrokers
  val rawTopic: String = setRawTopic
  val hookPath: String = setHookPath
  val landingPath: String = setLandingPath
  val landingTable: String = setLandingTable

  /** function that instantiates Kafka Consumer. It takes two input parameters
   * @param topic topic name for Kafka Consumer to follow
   * @param group name of the Kafka Consumer group  to which the created Kafka Consumer instance have to join
   */
  def runConsumer(topic: String = rawTopic, group: String = groupId, shutHook: String = hookPath): Unit = {

    var emptyFrames = 0 // empty Dataframes counter
    // setting Kafka Consumer config parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> offsetMode,
      "enable.auto.commit" -> (true: java.lang.Boolean),
      "auto.commit.interval.ms" -> commitInterval
    )
    // subscribing Consumer to kafka topic
    val stream = KafkaUtils.createDirectStream[String, String](
      ConsumerApp.ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
    // selecting Consumer Record fields that will be saved to landing layer
    val streamRec = stream.map(record => (record.topic, record.timestamp(), record.value))

    streamRec.foreachRDD({ rdd =>
      // instantiating Spark session to save polled consumer records to Spark Dataframe
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf)
        .enableHiveSupport()
        // enabling hive dynamic partitioning
        .config("hive.exec.dynamic.partition", setHivePartition)
        .config("hive.exec.dynamic.partition.mode", setPartitionMode)
        .config("hive.prewarm.enabled", setHivePrewarm)
        .config("hive.prewarm.numcontainers", setHiveContainers)
        .getOrCreate()

      import spark.implicits._

      // writing Kafka consumer record to dataframe
      val kConsumerDF = rdd.toDF("topic", "time_id", "full_tweet")
        // adding datetime columns for Hive partitioning
        .withColumn("year", year(from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz)))
        .withColumn("month", month(from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz)))
        .withColumn("day", dayofmonth(from_utc_timestamp(to_utc_timestamp(current_timestamp(), tz_local), tz)))
      val recCount = kConsumerDF.count()

      // saving Dataframe only if it contains any records
      if (recCount > 0) {
        kConsumerDF
          .write
          .partitionBy("year", "month", "day")
          .format("hive")
          .mode("append")
          .option("path", landingPath)
          .saveAsTable(landingTable)

        //kConsumerDF.show()
        val recCount = kConsumerDF.count()
        println(s" $recCount tweet records saved to hive table")
        emptyFrames = 0
      } else {
        emptyFrames += 1
        // create shutdown hook when more than five empty dataframes have been generated in this streaming context
        if (emptyFrames > 5) {
          val stop = Seq(shutdownString).toDF().coalesce(1)
          stop.write.mode("overwrite").text(hookPath)
        }
      }
    })
  }
}
