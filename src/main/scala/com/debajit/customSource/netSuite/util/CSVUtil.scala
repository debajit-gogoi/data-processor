package com.debajit.customSource.netSuite.util

import com.typesafe.scalalogging.LazyLogging

/**
 * @author debajit
 */
object CSVUtil extends LazyLogging {

  def readCSV(csvLocation: String): Map[String, String] = {
    var resultMap: Map[String, String] = Map.empty

    if (csvLocation != null) {
      val bufferedSource = scala.io.Source.fromFile(csvLocation)

      for (line <- bufferedSource.getLines) {
        if (!line.startsWith("#")) {
          val cols = line.split(",").map(_.trim)
          if (cols.length != 2) {
            throw new Exception("Invalid Row : " + line + "\n Please make sure rows are specified as 'Key','Value' in " + csvLocation)
          }

          resultMap += cols(0) -> cols(1)
        } else {
          logger.info("Ignoring commented line " + line)
        }
      }
      bufferedSource.close()
    }

    resultMap
  }

}
