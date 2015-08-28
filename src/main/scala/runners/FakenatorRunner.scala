package runners

import java.util.UUID

import models.RawModel
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.Serialization._


object FakenatorRunner {

  val NumberOfRaws = 1

  def main(args: Array[String]): Unit = {

    implicit val formats: Formats = DefaultFormats

    var clientId_CreditCard: Map[Int, String] = Map()

    (1 to 30000).foreach(x => {
      clientId_CreditCard = clientId_CreditCard + (x -> RawModel.generateCreditCard(""))
    })

    (1 to NumberOfRaws).foreach(x => {
      val id = UUID.randomUUID().toString
      val timestamp = RawModel.generateTimestamp()
      val client_id = RawModel.generateRandomInt(1, 30000)
      val latitude = 2d
      val longitude = 2d
      val payment_method = RawModel.generatePaymentMethod()
      val credit_card = clientId_CreditCard.get(client_id).get
      val shopping_center = RawModel.generateShoppingCenter()
      val employee = RawModel.generateRandomInt(1, 300)

      val lines = RawModel.generateLines()
      val total_amount = lines.map(x => x.price * x.quantity).sum

      val rawModel = new RawModel(
        id,
        timestamp,
        client_id,
        latitude,
        longitude,
        payment_method,
        credit_card,
        shopping_center,
        employee,
        total_amount,
        lines)

      println(write(rawModel))

    })
  }
}
