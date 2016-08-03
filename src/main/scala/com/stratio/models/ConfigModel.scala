package com.stratio.models

import com.stratio.models.ConfigModel._

case class ConfigModel(hostname: String = "localhost",
                       port: Int = 4141,
                       rawSize: Int = DefaultRawSize,
                       rawTimeout: Long = DefaultRawSizeTimeout,
                       generateAlert: Int = DefaulGenerateAlert) {}

object ConfigModel {

  val DefaulGenerateAlert = -1
  val DefaultRawSize = 10
  val DefaultRawSizeTimeout = 100L
}