package com.stratio.runners

import java.util.UUID
import com.stratio.models.RawModel
import org.apache.log4j.Logger
import org.json4s.native.Serialization._
import org.json4s.{DefaultFormats, Formats}


object FakenatorRunner {

  val NumberOfRawToGenerate = 1

  val NumberOfClients = 30000

  def main(args: Array[String]) {

    implicit val formats: Formats = DefaultFormats

    val L = Logger.getLogger(FakenatorRunner.getClass)

    var clientIdCreditCard: Map[Int, String] = Map()

    (1 to NumberOfClients).foreach(x => {
      clientIdCreditCard = clientIdCreditCard + (x -> RawModel.generateCreditCard(""))
    })

    (1 to NumberOfRawToGenerate).foreach(x => {
      val id = UUID.randomUUID().toString
      val timestamp = RawModel.generateTimestamp()
      val clientId = RawModel.generateRandomInt(1, NumberOfClients)
      val latitude = 2d
      val longitude = 2d
      val paymentMethod = RawModel.generatePaymentMethod()
      val creditCard = clientIdCreditCard.get(clientId).get
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

      L.info(write(rawModel))
      println(write(rawModel))
    })
  }
}
