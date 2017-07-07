package com.hashmap.tempus.processors

import java.io.File
import be.cetic.tsimulus.config.Configuration
import be.cetic.tsimulus.timeseries._
import com.github.nscala_time.time.Imports._
import spray.json._
import scala.io.Source

object SimController
{
  /**
    * Returns a point in time value for all exported values in the configuration file
    * @param ts The value in time to generate data for
    * @return a scala Iterable of Tuples in the form of URI, Timestamp, value
    */
  def getTimeValue(ts: Map[String, (TimeSeries[Any], _root_.com.github.nscala_time.time.Imports.Duration)], genTime: LocalDateTime) : Iterable[(String,LocalDateTime,AnyRef)] =
  {
    ts.map(series =>
    {
      val values = series._2._1
      val time = genTime
      val data = values.compute(time)
      new Tuple3[String,LocalDateTime,AnyRef](series._1, time, data)
    })

  }

  /**
    * Wrapped scala method to be able to be accessed from Java
    * @param filePath The path to the configuration file
    * @return The configuration object parsed from the file
    */
  def getConfiguration(filePath: String): Configuration = {
    val content = Source .fromFile(new File(filePath))
      .getLines()
      .mkString("\n")

    Configuration(content.parseJson);
  }

}
