import java.util.{ HashMap => JMap }
import java.util.UUID

import scala.concurrent.Future

import akka.actor.{ ActorSystem }
import akka.stream._
import scaladsl._

import org.apache.kafka.clients.producer._

object IntoKafka extends App {
  implicit val system = ActorSystem("IntoKafkaSystem")
  implicit val materializer = ActorFlowMaterializer()
  implicit val ec = materializer.executionContext

  val kafkaProducer = Kafka.producer

  val source = Source(1 to 10)

  val kafkaSink = Sink.foreach[ProducerRecord[Array[Byte], Array[Byte]]](kafkaProducer.send(_))

  val flow = source
                .map(i => UUID.randomUUID)
                .map(_.toString.getBytes)
                .map(Kafka.toRecord)
              .toMat(kafkaSink)(Keep.right)

  flow.run().foreach(s => system.shutdown)
}

object Kafka {
  val MetadataBrokerList = "metadata.broker.list"
  val RequestRequiredAcks = "request.required.acks"
  val ProducerType = "producer.type"
  val SerializerClass = "serializer.class"
  val BootstrapServers = "bootstrap.servers"
  val KeySerializer = "key.serializer"
  val KeySerializerClass = "key.serializer.class"
  val ValueSerializer = "value.serializer"

  val Async = "async"
  val Sync = "sync"

  val ByteArraySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"

  val brokerList = "192.168.59.103:9092"
  val topicName = "test"

  lazy val producer: Producer[Array[Byte], Array[Byte]] = {
    val config = new JMap[String, Object]()
    config.put(BootstrapServers, brokerList)
    config.put(ProducerType, Sync)
    config.put(KeySerializer, ByteArraySerializer)
    config.put(ValueSerializer, ByteArraySerializer)

    new KafkaProducer(config)
  }

  def toRecord[V](value: V) = new ProducerRecord[V, V](topicName, value)
}
