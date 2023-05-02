package com.debajit.dataProcessor.sink

import com.debajit.dataProcessor.utils.Utils
import com.debajit.dataProcessor.utils.Utils.getColsSeqString
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

/**
 * @author Debajit
 */

object FileSink extends Sink {

  /**
   * write dataframe to file
   *
   * @param df             data frame
   * @param sinkProperties sink properties
   */

  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {
    val toWrite: DataFrameWriter[Row] = if (sinkProperties.properties.contains("partition_cols")) {
      val partitionCols = getColsSeqString(sinkProperties.properties.get("partition_cols"))
      df.write partitionBy (partitionCols.get: _*)
    }
    else df.write
    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }
    val format = sinkProperties.properties("format")
    val location = sinkProperties.properties("output_location")
    toWrite format format mode mode option("path", location) save()
  }
}
