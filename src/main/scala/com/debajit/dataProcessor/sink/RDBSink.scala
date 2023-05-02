package com.debajit.dataProcessor.sink

import com.amazonaws.services.athena.model.PreparedStatement
import com.debajit.dataProcessor.source.{RDBSource, SourceProperties}
import com.debajit.dataProcessor.utils.Utils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

import java.sql.{Connection, DriverManager, Statement}
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util
import java.util.Properties

/**
 * @author debajit
 */
object RDBSink extends Sink {
  @throws(classOf[Exception])
  override def write(df: DataFrame, sinkProperties: SinkProperties): Unit = {

    val dfToWrite = (sinkProperties.properties contains "cast_to_table_schema") && (sinkProperties properties "cast_to_table_schema" equalsIgnoreCase "true") match {
      case true => castToSchema(df, sinkProperties)
      case false => df
    }


    if (sinkProperties.properties contains "last_n_days") {
      deleteLastNDaysData(sinkProperties properties "url", sinkProperties properties "user", sinkProperties properties "password",
        sinkProperties properties "driver", Integer parseInt sinkProperties.properties("last_n_days"), sinkProperties properties "table_name")
    }

    val mode = sinkProperties.properties contains "mode" match {
      case false => SaveMode.Append
      case true => Utils getSaveMode sinkProperties.properties("mode")
    }
    val url = sinkProperties.properties("url")
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", sinkProperties properties "driver")
    connectionProperties.put("user", sinkProperties properties "user")
    connectionProperties.put("password", sinkProperties properties "password")
    connectionProperties.put("dbname", sinkProperties properties "database")
    connectionProperties.put("url", sinkProperties properties "url")
    val tableName = sinkProperties properties "table_name"
    val operation = sinkProperties properties "operation"
    operation match {
      case "insert" => dfToWrite.write.mode(mode).jdbc(url, tableName, connectionProperties)
      case "upsert" =>
        connectionProperties.put("key_cols", sinkProperties properties "key_cols")
        upsertRDB(dataFrame = dfToWrite, connectionProperties = connectionProperties)
      case "merge" =>
    }
  }

  def castToSchema(df: DataFrame, sinkProperties: SinkProperties): Dataset[Row] = {
    val tableName = sinkProperties.properties("table_name")
    val queryForSchema = s"select * from $tableName limit 1"
    val sourceProperties = (sinkProperties.properties filter (entry => entry._1 != "query")) + ("query" -> queryForSchema)
    val tableSchema = (RDBSource read(SourceProperties(sourceProperties), None, None))._1.schema

    val columns = df.columns.toList.map(column => column.toLowerCase)
    val structType = tableSchema filter (field => columns contains field.name.toLowerCase)

    df.selectExpr(structType.map(
      field => s"CAST ( ${field.name} As ${field.dataType.sql}) ${field.name}"
    ): _*)
  }

  def upsertRDB(dataFrame: DataFrame, connectionProperties: Properties): Unit = {
    val sc = dataFrame.sparkSession.sparkContext
    connectionProperties.put("columns_dtype", dataFrame.dtypes)
    connectionProperties.put("columns", dataFrame.columns.mkString(","))
    val brConnect = sc.broadcast(connectionProperties)
    dataFrame.coalesce(10).foreachPartition(partition => {
      val connectionProperties = brConnect.value
      val url = connectionProperties getProperty "url"
      val user = connectionProperties getProperty "user"
      val password = connectionProperties getProperty "password"
      val driver = connectionProperties getProperty "Driver"
      val key_cols = connectionProperties getProperty "key_cols"
      val columns = connectionProperties getProperty "columns"
      val out = (connectionProperties get "columns_dtype").asInstanceOf[Array[(String, String)]].toMap
      Class.forName(driver)
      val dbc = DriverManager.getConnection(url, user, password)
      val db_batchsize = 100
      var st: PreparedStatement = null
      partition.grouped(db_batchsize).foreach(batch => {
        batch.foreach {
          row => {
            val listOfColumns = columns.split(",").toList
            val mapOfColumnValue = new util.HashMap[String, String]()

          }
        }
      })

    })
  }

  def deleteLastNDaysData(url: String, user: String, password: String, driver: String, noOfDays: Int, tableName: String): Unit = {
    val date = LocalDateTime.of(LocalDate.now(ZoneOffset.UTC) minusDays noOfDays, LocalTime.MIDNIGHT)
    println(s"Deleting data for last $noOfDays days from table = $tableName and date >=  $date")
    Class forName driver
    val dbc: Connection = DriverManager getConnection(url, user, password)
    val sqlString = s"delete from $tableName where systemmodstamp >='$date'"
    val statement: Statement = dbc createStatement()
    val result: Boolean = statement execute sqlString
    println(s"Data deleted : ${result.toString}")
    dbc close()
  }
}
