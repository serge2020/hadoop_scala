package top_tweets

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/** class constructor used to instantiate KafkaProducer */
class TopTweetsProducer extends Settings {
  // loading Kafka params
  val brokers: String = setBrokers
  val rawTopic: String = setRawTopic

  /** @return this method builds a KafkaProducer instance with supplied Kafka params
   */
  def kafkaProducer: KafkaProducer[String, String] = {
    val props = new Properties()
    val serde = setSerializer
    val acks = setAcks
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.ACKS_CONFIG, acks)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serde)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serde)
    new KafkaProducer[String, String](props)
  }
  /** method that takes
   * @param record  a string as input
   * @return transforms it to ProducerRecord instance and sends it to Kafka server */
  def sendRecord(record: String):Future[RecordMetadata] = {
    val kafkaRec = new ProducerRecord[String, String](rawTopic, null, record)
    kafkaProducer.send(kafkaRec)
  }
  /** implementation of the standart KafkaProducer.flush() method,
   *         flushes accumulated records in Kafka producer  */
  def flush(): Unit = kafkaProducer.flush()

  /** implementation of the standart KafkaProducer.close() method,
   *          closes KafkaProduce instance */
  def close(): Unit = kafkaProducer.close()
}
