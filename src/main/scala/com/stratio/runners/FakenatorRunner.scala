/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.runners

import java.util.UUID

import com.stratio.kafka.KafkaProducer
import com.stratio.models.{ConfigModel, RawModel}
import org.json4s.native.Serialization._
import org.json4s.{DefaultFormats, Formats}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.{Failure, Try}
import org.apache.log4j.Logger

object FakenatorRunner {

  implicit val formats: Formats = DefaultFormats

  val DefaultFailureTimeout = 2000L
  val NumberOfClients = 1000
  val geolocations = generateGeolocations()
  val clientIdCreditCard: Map[Int, String] = generateClientIdCreditCard((1 to NumberOfClients).toSeq, Map())
  val clientIdGeo: Map[Int, (Double, Double)] = generateClientIdGeo(clientIdCreditCard, geolocations)

  val logger = Logger.getLogger(FakenatorRunner.getClass)

  val alertMessage = """
                      0: For the same client_id more than one order in less than 5 minutes with the same credit card in
                         different shopping centers.
                      1: For the same client_id more than one order with different credit cards in less than 10 minutes.
                     """

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[ConfigModel]("fakenator") {
      head("Stratio Fakenator", "1.0")
      opt[String] ('b', "bootstrapServers").required.action { (x, c) =>
        c.copy(bootstrapServers = x) } text(s"Bootstrap servers of confluent-kafka (mandatory)")
      opt[String] ('t', "topic").required.action { (x, c) =>
        c.copy(topic = x) } text(s"Bootstrap servers of confluent-kafka (mandatory)")
      opt[String] ('s', "schemaRegistryURL").required.action { (x, c) =>
        c.copy(schemaRegistryURL = x) } text(s"SchemaRegistryURL of confluent-schema (mandatory)")
      opt[Int] ('r', "rawSize") action { (x, c) =>
        c.copy(rawSize = x) } text(s"number of created events before to perform a timeout. Default: ${ConfigModel.DefaultRawSize}")
      opt[Int]('o', "rawTimeout") action { (x, c) =>
        c.copy(rawTimeout = x) } text(s"number of milliseconds to wait after events were created. Default: ${ConfigModel.DefaultRawSizeTimeout} milliseconds")
      help("help") text("prints this usage text")
    }

    parser.parse(args, ConfigModel()) match {
      case Some(config) => {
        Try({
          generateRaw(clientIdGeo, clientIdCreditCard, config, 1)
        }) match {
          case Failure(ex) => logger.error(ex.getLocalizedMessage, ex)
          case _ => None
        }
      }
      case None => parser.showTryHelp
    }
  }

  // XXX Private methods
  @tailrec
  private def generateRaw(clientIdGeo: Map[Int, (Double, Double)],
                          clientIdCreditCard: Map[Int, String],
                          config: ConfigModel,
                          count: Int)(implicit formats: Formats): Unit = {
    val id = UUID.randomUUID().toString
    val clientId = RawModel.generateRandomInt(1, NumberOfClients)
    val latitude = clientIdGeo.get(clientId).get._1
    val longitude = clientIdGeo.get(clientId).get._2
    val paymentMethod = RawModel.generatePaymentMethod()
    val creditCard = clientIdCreditCard.get(clientId).get
    val shoppingCenter = RawModel.generateShoppingCenter()
    val employee = RawModel.generateRandomInt(1, 300)

    val rawModel = new RawModel(
      id,
      clientId,
      latitude,
      longitude,
      paymentMethod,
      creditCard,
      shoppingCenter,
      employee)

    println(write(rawModel))

    val producer = KafkaProducer.getInstance(s"${config.bootstrapServers}", s"${config.schemaRegistryURL}")
    KafkaProducer.send(producer, config.topic, rawModel)

    if(count % config.rawSize == 0) {
      Thread.sleep(config.rawTimeout)
      generateRaw(clientIdGeo, clientIdCreditCard, config, 0)
    } else {
      generateRaw(clientIdGeo, clientIdCreditCard, config, count + 1)
    }
  }

  private def generateGeolocations() : Seq[String] = {
    Source.fromInputStream(
      this.getClass.getClassLoader.getResourceAsStream("geolocations.csv")).getLines().toSeq
  }

  private def generateClientIdCreditCard(idClients: Seq[Int],
                                         clientIdCreditCard: Map[Int, String]): Map[Int, String] = {
    if(idClients.size == 0) {
      clientIdCreditCard
    } else {
      val newIdClients = idClients.init
      val newClientIdCreditCard = clientIdCreditCard + (idClients.last -> RawModel.generateCreditCard(""))
      generateClientIdCreditCard(newIdClients, newClientIdCreditCard)
    }
  }

  private def generateClientIdGeo(clientIdCreditCard: Map[Int, String], geolocations: Seq[String])
  :Map[Int, (Double, Double)] = {
    clientIdCreditCard.map(x => {
      val index = RawModel.generateRandomInt(0, geolocations.size - 1)
      x._1 -> ((geolocations(index)).split(":")(0).toDouble, (geolocations(index)).split(":")(1).toDouble)
    })
  }
}
