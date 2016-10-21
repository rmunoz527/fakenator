package com.stratio.constants


object FakenatorConstants {

  val AvroSchema =
    """
      |{
      |  "type": "record",
      |  "name": "rawModel_schema",
      |  "fields": [
      |    {"name": "order_id", "type": "string"},
      |    {"name": "client_id", "type": "int"},
      |    {"name": "latitude", "type": "double"},
      |    {"name": "longitude", "type": "double"},
      |    {"name": "payment_method", "type": "string"},
      |    {"name": "credit_card", "type": "string"},
      |    {"name": "shopping_center", "type": "string"},
      |    {"name": "employee", "type": "int"}
      |  ]
      |}
    """.stripMargin
}
