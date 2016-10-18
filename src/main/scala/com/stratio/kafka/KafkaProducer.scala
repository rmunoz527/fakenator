//package com.stratio.kafka
//
//import java.util.Properties
//
//import com.typesafe.config.Config
//import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
//
//object KafkaProducer {
//
//  def getInstance(config: Config): Producer[String, String] = {
//    val props: Properties = new Properties()
//    props.put("metadata.broker.list", config.getString("brokerList"))
//    props.put("serializer.class", "kafka.serializer.StringEncoder")
//    props.put("request.required.acks", "1")
//
//    val producerConfig = new ProducerConfig(props)
//    new Producer[String, String](producerConfig)
//  }
//
//  def send(producer: Producer[String, String], topic: String, message: String): Unit = {
//    val keyedMessage: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, message)
//    producer.send(keyedMessage)
//  }
//}
