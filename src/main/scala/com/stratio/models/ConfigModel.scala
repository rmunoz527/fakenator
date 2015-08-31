package com.stratio.models

import com.stratio.models.ConfigModel._

case class ConfigModel(rawSize: Int = DefaultRawSize, rawTimeout: Long = DefaultRawSizeTimeout) {}

object ConfigModel {

  val DefaultRawSize = 10
  val DefaultRawSizeTimeout = 100L
}