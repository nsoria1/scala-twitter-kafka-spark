package tests

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.mockito.MockitoSugar._

class KafkaDataProducerTest extends AnyFunSpec {
  it("should create a valid Kafka Producer object") {
    import com.nsoria.KafkaDataProducer
    import org.apache.kafka.clients.producer.KafkaProducer

    // Mock Kafka connection
    val mockKafka = mock[KafkaProducer[String, String]]

    // Assert validation
    assert(KafkaDataProducer.getKafkaProducer().isInstanceOf[KafkaProducer[String, String]] == mockKafka.isInstanceOf[KafkaProducer[String, String]])
  }
}
