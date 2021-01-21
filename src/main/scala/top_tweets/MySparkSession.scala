package top_tweets

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MySparkSession extends Settings {

  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    // setting hive parameters that allow dynamic partitioning
    .config("hive.exec.dynamic.partition", setHivePartition)
    .config("hive.exec.dynamic.partition.mode", setPartitionMode)
    .config("hive.prewarm.enabled", setHivePrewarm)
    .config("hive.prewarm.numcontainers", setHiveContainers)
    // and vectorized join execution for improved join query performance
    .config("hive.vectorized.execution.enabled", setVectQuery)
    // the dafault shuffle partition number is 200. There is no need to have so many partitions for the amount of data processed
    .config("spark.sql.shuffle.partitions", setSqlPartitions)
    .master(setMaster)
    .getOrCreate()

  val conf: SparkConf = new SparkConf().setMaster(setMaster)

}
