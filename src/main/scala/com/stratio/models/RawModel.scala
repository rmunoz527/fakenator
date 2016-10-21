/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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
package com.stratio.models

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

case class RawModel (order_id: String,
                     client_id: Integer,
                     latitude: Double,
                     longitude: Double,
                     payment_method: String,
                     credit_card: String,
                     shopping_center: String,
                     employee: Integer) {}

case class LineModel (product: String,
                      family: String,
                      quantity: Integer,
                      price: Float)

object RawModel {

  val MaxLines = 30
  val Range_client_id = (1, 30000)
  val Range_payment_method = Source.fromInputStream(
  this.getClass.getClassLoader.getResourceAsStream("payment-methods.txt")).getLines().toSeq
  val Range_shopping_center = Source.fromInputStream(
    this.getClass.getClassLoader.getResourceAsStream("shopping-centers.txt")).getLines().toSeq
  val Range_employee = (1, 300)
  val Range_quantity = (1, 30)
  val R = Random

  val Range_family_product: Map[String, Map[String,Float]] = Source.fromInputStream(
    this.getClass.getClassLoader.getResourceAsStream("family-products.csv")).getLines().map(x => {
      val splitted = x.split(",")
      (splitted(0), Map(splitted(1) -> splitted(2).toFloat))
    }).toMap

  def generateLines(): Seq[LineModel] = {
    (1 to generateRandomInt(1,MaxLines)).map(x => {
      val family = Range_family_product.keySet.toSeq(generateRandomInt(0, Range_family_product.keySet.size - 1))
      val product: String = Range_family_product.get(family)
        .get.keySet.toSeq(generateRandomInt(0, Range_family_product.get(family).get.keySet.size - 1))
      val price: Float = Range_family_product.get(family).get.get(product).get
      val quantity = generateRandomInt(1, 30)
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
    val datetime = new DateTime().minusDays(generateRandomInt(0,60))
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss ZZ").print(datetime)
  }

  def generateRandomInt(min: Int, max: Int): Int = {
    R.nextInt((max -min) + 1) + min
  }

  @tailrec
  def generateCreditCard(current: String): String = {
    if(current.length != 16) generateCreditCard(current + generateRandomInt(0,9))
    else current
  }
}