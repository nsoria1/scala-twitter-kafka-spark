package com.nsoria

object Variables {

  // Spark Variables
  val APP_NAME = "spark-kafka-postgresql"
  val SPARK_MASTER = "local[*]"
  val CHECKPOINT_PATH = "checkpoints/"
  val ERROR_LOG_LEVEL = "INFO"
  //val EXECUTION_MODE: String = scala.util.Properties.envOrElse("EXECUTION_MODE", "console")
  val EXECUTION_MODE = "db"

  // Kafka Variables
  val BROKER: String = scala.util.Properties.envOrElse("KAFKA_BROKER", "localhost:9092")
  val TOPIC: String = scala.util.Properties.envOrElse("KAFKA_TOPIC", "twitter")

  // Postgres Variables
  val POSTGRES_URL = scala.util.Properties.envOrElse("POSTGRES_URL", "jdbc:postgresql://localhost:5432/postgres")
  val POSTGRES_USER = "postgres"
  val POSTGRES_PW = "postgres"
}
