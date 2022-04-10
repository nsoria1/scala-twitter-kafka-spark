package main.twitter_kafka.app

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

object KafkaProducer extends App {

  // Define Kafka cluster parameters
  val kafkaCfg = {
    val settings = new Properties()
    settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
    settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    settings
  }
  val producer = new KafkaProducer[String, String](kafkaCfg)

  // Parameters hardcoded
  val topic = "twitter"
  val query = "bad bunny"
  val times = 5

  println("Getting Twitter records ...")
  for (i <- 1 to times) {
    val records = TwitterGetter.main(query)
    try {
      for (item <- records) {
        println("Sending records to Kafka cluster ...")
        println(item)
        val record = new ProducerRecord[String, String]("twitter", "bad bunny", item.toString )
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
  producer.close()
}
