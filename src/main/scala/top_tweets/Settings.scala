package top_tweets

import com.typesafe.config.{Config, ConfigFactory}

/** trait that loads setting parameters from top_tweets.conf file
 */
trait Settings extends Serializable {

  // set .conf file path and load twitter, kafka and hive params to Config variables
  val config: Config = ConfigFactory.load()

  val kConfig: Config = config.getConfig("kafka")
  val sConfig: Config = config.getConfig("spark")
  val tConfig: Config = config.getConfig("twitter")
  val hConfig: Config = config.getConfig("hive")

  // set Kafka broker params and file path for Spark Streaming shutdown hook
  val setBrokers: String = kConfig.getString("brokers")
  val setSerializer: String = kConfig.getString("serializer")
  val setAcks: String = kConfig.getString("acks")
  val groupId: String = kConfig.getString("group_id")
  val offsetMode: String = kConfig.getString("offset_reset")
  val commitInterval: String = kConfig.getString("commit_interval")
  val setRawTopic: String = kConfig.getString("raw_topic")
  val setHookPath: String = kConfig.getString("shutdown_hook")
  val shutdownString: String = kConfig.getString("shutdown_string")

  // set Spark Session and Spark Streaming params
  val appName: String = sConfig.getString("app_name")
  val setMaster: String = sConfig.getString("master")
  val setHivePartition: String = sConfig.getString("hive_partition")
  val setPartitionMode: String = sConfig.getString("hive_partmode")
  val setHivePrewarm: String = sConfig.getString("hive_prewarm")
  val setHiveContainers: String = sConfig.getString("hive_containers")
  val setVectQuery: String = sConfig.getString("hive_vectquery")
  val setSqlPartitions: String = sConfig.getString("spark_partitions")
  val batchSeconds: Long = sConfig.getInt("batch_sec").toLong

  // set tweet filter params,  number of tweets to collect and load access keys & tokens from top_tweets.conf
  val setLanguage: String = tConfig.getString("language")
  val setMinRetweets: Int = tConfig.getInt("min_retweets")
  val setNumTweetsToCollect: Int = tConfig.getInt("num_tweets")
  val setConsumerKey: String = tConfig.getString("consumer_key")
  val setConsumerSecret: String = tConfig.getString("consumer_secret")
  val setToken: String = tConfig.getString("token")
  val setTokenSecret: String = tConfig.getString("token_secret")
  val setQueLength: Int = tConfig.getInt("que_capacity")

  // set Hive table folder paths and table names
  val setLandingPath: String = hConfig.getString("landing_path")
  val setLandingTable: String = hConfig.getString("landing_table")
  val setBasePath: String = hConfig.getString("base_path")
  val setBaseTable: String = hConfig.getString("base_table")
  val setAnalyticPath: String = hConfig.getString("analytic_path")
  val setAnalyticTable: String = hConfig.getString("analytic_table")
  val setAnalyticView: String = hConfig.getString("analytic_view")
  val setAnalyticStaging: String = hConfig.getString("analytic_staging")
  val daysBack1: Int = hConfig.getInt("days_filter1")
  val daysBack2: Int = hConfig.getInt("days_filter2")
  val tz: String = hConfig.getString("time_zone")

 // set local time-zone
  val tz_local: String = System.getProperty("user.timezone")
 }
