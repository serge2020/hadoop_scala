package top_tweets

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Standalone application to launch Kafka Consumer instances */
object ConsumerApp extends Log4jSupport with Settings {

  //val configFile: String = confFilePath
  val hookPath: String = setHookPath
  val master: String = setMaster
  println(master)

  // creating Spark streaming context
  val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
  val ssc = new StreamingContext(conf, Seconds(batchSeconds))

  def main(args: Array[String]) {

    // launching Kafka Consumer instance by instantiating TopTweetsConsumer class
    val consumerOne = new TopTweetsConsumer
    consumerOne.runConsumer()

    /** multiple Kafka Consumer instances can be launched here to read from selected partitions or topics */

    // starting streaming context
    ssc.start()

    // setting shutdown hook conditions
    val fs = FileSystem.get(ssc.sparkContext.hadoopConfiguration)

    var stop: Boolean = false
    while (!stop) {
      Thread.sleep(5000)
      stop = fs.exists(new Path(hookPath))
      if (stop) {
        // shutting down if the hook is present
        println("terminating application, exiting gracefully...")
        fs.removeAcl(new Path(hookPath))
        stop = true
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
  }
}