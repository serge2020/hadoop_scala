package top_tweets

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

/** trait to manage log4j logging levels in all objects of the interns.top_tweets package */
trait Log4jSupport {

  Logger.getLogger("org").setLevel(Level.INFO)

}
