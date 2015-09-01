package com.stratio.runners

import java.util.UUID

import com.stratio.models.{ConfigModel, RawModel}
import org.apache.log4j.Logger
import org.json4s.native.Serialization._
import org.json4s.{DefaultFormats, Formats}

import scala.annotation.tailrec
import scala.io.Source

object FakenatorRunner {

  val DefaultFailureTimeout = 2000L
  val NumberOfClients = 1000

  implicit val formats: Formats = DefaultFormats

  val geolocations = generateGeolocations()
  val clientIdCreditCard: Map[Int, String] = generateClientIdCreditCard((1 to NumberOfClients).toSeq, Map())
  val clientIdGeo: Map[Int, (Double, Double)] = generateClientIdGeo(clientIdCreditCard, geolocations)

  val L = Logger.getLogger(FakenatorRunner.getClass)

  val alertMessage = """
                      0: For the same client_id more than one order in less than 5 minutes with the same credit card in
                         different shopping centers.
                      1: For the same client_id more than one order with different credit cards in less than 10 minutes.
                     """

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[ConfigModel]("fakenator") {
      head("Stratio Fakenator", "1.0")
      opt[Int] ('r', "rawSize") action { (x, c) =>
        c.copy(rawSize = x) } text(s"number of created events before to perform a timeout. Default: ${ConfigModel.DefaultRawSize}")
      opt[Int]('t', "rawTimeout") action { (x, c) =>
        c.copy(rawTimeout = x) } text(s"number of milliseconds to wait after events were created. Default: ${ConfigModel.DefaultRawSizeTimeout} milliseconds")
      opt[Int]('a', "generateAlert") action { (x, c) =>
        c.copy(generateAlert = x) } text(alertMessage)
      help("help") text("prints this usage text")
    }

    parser.parse(args, ConfigModel()) match {
      case Some(config) => {
        generateRaw(clientIdGeo, clientIdCreditCard, config, 1)
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
    val timestamp = RawModel.generateTimestamp()
    val clientId = if(config.generateAlert == 0 || config.generateAlert == 1) 10 else RawModel.generateRandomInt(1,
      NumberOfClients)
    val latitude = clientIdGeo.get(clientId).get._1
    val longitude = clientIdGeo.get(clientId).get._2
    val paymentMethod = RawModel.generatePaymentMethod()
    val creditCard = if(config.generateAlert == 1) RawModel.generateCreditCard("") else clientIdCreditCard.get(clientId).get
    val shoppingCenter = RawModel.generateShoppingCenter()
    val employee = RawModel.generateRandomInt(1, 300)

    val lines = RawModel.generateLines()
    val totalAmount = lines.map(x => x.price * x.quantity).sum

    val rawModel = new RawModel(
      id,
      timestamp,
      clientId,
      latitude,
      longitude,
      paymentMethod,
      creditCard,
      shoppingCenter,
      employee,
      totalAmount,
      lines)

    println(write(rawModel))
    L.info(write(rawModel))

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
