package models

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.{DefaultFormats, Formats}

import scala.util.Random

case class RawModel (id: String,
                     timestamp: String,
                     client_id: Integer,
                     latitude: Double,
                     longitude: Double,
                     payment_method: String,
                     credit_card: String,
                     shopping_center: String,
                     employee: Integer,
                     total_amount: Float,
                     lines: Seq[LineModel]) {}

case class LineModel (product: String,
                      family: String,
                      quantity: Integer,
                      price: Float)

object RawModel {

  val MaxLines = 30
  val Range_client_id = (1, 30000)
  val Range_payment_method = Seq("credit card", "cash", "online")
  val Range_shopping_center = Seq("Sevilla", "Madrid", "Salamanca", "Valencia", "Barcelona", "Rome", "Paris", "Online")
  val Range_employee = (1, 300)
  val Range_quantity = (1, 30)
  val R = Random

  val Range_family_product: Map[String, Map[String,Float]] = Map(
    "electronic" -> Map(
      "keyboard" -> 30.5f,
      "ram" -> 90f
    ),
    "feeding" -> Map(
      "apples" -> 5.3f,
      "milk" -> 3f
    ),
    "clothes" -> Map(
      "nike air shoes" -> 120.82f,
      "adidas shoes" -> 90.23f
    ),
    "drugs" -> Map(
      "aspirin" -> 5.6f,
      "sunscreen" -> 3f
    )
  )

  def generateLines(): Seq[LineModel] = {
    (1 to MaxLines).map(x => {
      val family = Range_family_product.keySet.toSeq(generateRandomInt(0, Range_family_product.keySet.size - 1))
      val product: String = Range_family_product.get(family)
        .get.keySet.toSeq(generateRandomInt(0, Range_family_product.get(family).get.keySet.size - 1))
      val price: Float = Range_family_product.get(family).get.get(product).get
      val quantity = generateRandomInt(1, 8)
      new LineModel(product, family, quantity, price)
    })
  }

  def generateShoppingCenter(): String = {
    Range_shopping_center(generateRandomInt(0, Range_shopping_center.length - 1))
  }

  def generatePaymentMethod(): String = {
    Range_payment_method(generateRandomInt(0, Range_payment_method.length - 1))
  }

  def generateTimestamp(): String = {
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(new DateTime())
  }

  def generateRandomInt(min: Int, max: Int): Int = {
    R.nextInt((max -min) + 1) + min
  }

  def generateCreditCard(current: String): String = {
    if(current.length != 16) generateCreditCard(current + generateRandomInt(0,9))
    else current
  }

}