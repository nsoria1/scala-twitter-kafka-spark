package com.nsoria

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties

object KafkaDataProducer extends App {

  // Method to open Kafka connection
  def getKafkaProducer(): KafkaProducer[String, String] = {
    // Define Kafka cluster parameters
    val kafkaCfg = {
      val settings = new Properties()
      settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      settings
    }
    val producer = new KafkaProducer[String, String](kafkaCfg)
    producer
  }

  // Method to send data to Kafka
  def processData(producer: KafkaProducer[String, String], twitter: String, query: String, times: Int): Unit = {
    // Sending data to Kafka
    println("Getting Twitter records ...")
    for (i <- 1 to times) {
      val records = TwitterGetter.main(query)
      try {
        for (item <- records) {
          println(s"Sending records to Kafka cluster for ${i} time ...")
          println(item)
          val record = new ProducerRecord[String, String](
            twitter, query, item
          )
          producer.send(record)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    producer.close()
  }

  // Parameters hardcoded
  val twitter = if (args.size == 0) "twitter" else args(0)
  val query = if (args.size == 0) "bad bunny" else args(1)
  val times = if (args.size == 0) 5 else args(2).toInt

  // Get Producer
  val producer = getKafkaProducer()

  // Send data
  processData(producer, twitter, query, times)

}
