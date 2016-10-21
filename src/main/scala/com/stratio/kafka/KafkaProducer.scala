package com.stratio.kafka

import java.util.Properties

import com.stratio.constants.FakenatorConstants
import com.stratio.models.RawModel
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.{Failure, Success, Try}

object KafkaProducer {

  var kafkaProducerInstance: Option[KafkaProducer[Object, Object]] = None

  def getInstance(bootstrapServers: String, schemaRegistryURL: String): KafkaProducer[Object, Object] = {
    kafkaProducerInstance match {
      case None =>
        val props = new Properties()
        props.put("bootstrap.servers", bootstrapServers)
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        props.put("schema.registry.url", schemaRegistryURL)
        val kafkaProducer = new KafkaProducer[Object, Object](props)
        kafkaProducerInstance = Option(kafkaProducer)
        kafkaProducer
      case Some(value) => value
    }
  }

  def send(producer: KafkaProducer[Object, Object], topic: String, rawModel: RawModel): Unit = {
    val schema = new Schema.Parser().parse(FakenatorConstants.AvroSchema)

    val rawModelGenericMadrid = new GenericData.Record(schema)
    rawModelGenericMadrid.put("order_id", rawModel.order_id)
    rawModelGenericMadrid.put("client_id", rawModel.client_id)
    rawModelGenericMadrid.put("latitude", rawModel.latitude)
    rawModelGenericMadrid.put("longitude", rawModel.longitude)
    rawModelGenericMadrid.put("payment_method", rawModel.payment_method)
    rawModelGenericMadrid.put("credit_card", rawModel.credit_card)
    rawModelGenericMadrid.put("shopping_center", rawModel.shopping_center)
    rawModelGenericMadrid.put("employee", rawModel.employee)

    val producerRecord = new ProducerRecord[Object, Object] (topic, rawModelGenericMadrid)

    Try({
      producer.send(producerRecord)
    }) match {
      case Failure(ex) => ex.printStackTrace()
      case Success(value) => None
    }
  }
}
