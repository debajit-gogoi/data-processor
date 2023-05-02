package com.debajit.dataProcessor.utils

import com.github.dikhan.pagerduty.client.events.PagerDutyEventsClient
import com.github.dikhan.pagerduty.client.events.domain.{EventResult, Payload, Severity, TriggerIncident}
import com.debajit.dataProcessor.sink.SinkProperties
import com.debajit.dataProcessor.source._
import com.debajit.dataProcessor.spark.SparkHelper.getSparkSession
import com.debajit.dataProcessor.structs.LogMessage
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import java.io.File
import java.sql.{Connection, DriverManager}
import java.time.OffsetDateTime
import java.util
import java.util.Base64
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable.Stream.continually
import scala.collection.mutable
import scala.io.Source
import scala.reflect.runtime.universe
import scala.util.Random.nextInt
import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

/**
 * @author Debajit
 */

object Utils extends LazyLogging {

  /**
   * identifier for variable to replace value in run time
   */
  val variableIdentifier = "#"
  /**
   * streaming context
   */
  var ssc: StreamingContext = _

  /**
   * resolve config #tagged parameters with config values and properties map
   *
   * @param config      config value
   * @param variableMap properties map
   * @return
   */
  def resolveConfigVariables(config: String, variableMap: Map[String, String]): String = (variableMap foldLeft config) { (acc, x) => acc replaceAll(x._1 patch(0, variableIdentifier, 0), x._2) }

  /**
   * get method execution environment through reflection
   *
   * @param class_name  class name
   * @param method_name method name
   * @return
   */
  def getMethodThroughScalaReflection(class_name: String, method_name: String): universe.MethodMirror = {
    val rm: universe.Mirror = universe runtimeMirror getClass.getClassLoader
    val method_to_call: universe.MethodSymbol = ((rm staticClass class_name).selfType decl universe.TermName(method_name)).asMethod
    rm reflect (Class forName class_name newInstance()) reflectMethod method_to_call
  }

  def getUniqueTableName: String = {
    continually(nextInt(100)) take 10 mkString "_"
  }

  /**
   * get metadata sync db connection
   *
   * @return
   */
  def getMetaDataConnection: Connection = {
    val url = "jdbc:mysql://localhost:3306/data_processor"
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "root"
    val password = "****@Nar***en****92"
    var connection: Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      connection
    } catch {
      case e: Exception => e.printStackTrace()
        connection
    }

  }

  /**
   * get the save mode, by default append
   *
   * @param mode save mode
   * @return
   */
  def getSaveMode(mode: String): SaveMode = mode match {
    case "append" => SaveMode.Append
    case "overwrite" => SaveMode.Overwrite
    case "ignore" => SaveMode.Ignore
    case "errorifexists" => SaveMode.ErrorIfExists
    case _ => SaveMode.Append
  }

  /**
   * get spark app run env
   *
   * @param config config
   * @param key    key to search
   * @return
   */
  def getAppRunEnv(config: String, key: String): Option[String] = ((JSON parseFull readConfigFileAsString(config)).get.asInstanceOf[Map[String, Any]] get key).asInstanceOf[Option[String]]

  /**
   * get and map enabled pipelines
   *
   * @param config config
   * @return
   */
  def getEnabledPipelines(config: String): Map[String, Map[String, String]] = {
    val configs = readJsonConfig(config)
    val mapOfPipelines = (configs.get asInstanceOf[Map[String, Any]] "process" asInstanceOf[Map[String, Any]] "actions")
      .asInstanceOf[Map[String, Any]]
    val pipeline_properties: mutable.Map[String, Map[String, String]] = scala.collection.mutable.Map[String, Map[String, String]]()
    mapOfPipelines foreach { case (key, value) => mapEnabledPipelines(pipeline_properties, key, value) }
    pipeline_properties.toMap
  }

  /**
   * retrieve for enabled pipelines
   *
   * @param pipeline_properties pipeline properties
   * @param key                 key
   * @param value               value
   */
  def mapEnabledPipelines(pipeline_properties: mutable.Map[String, Map[String, String]], key: String, value: Any): Unit = {
    (value asInstanceOf[Map[String, Any]] "isEnabled").asInstanceOf[String] match {
      case "true" => println(s"$key is enabled!!")
        pipeline_properties += (s"$key" -> (value asInstanceOf[Map[String, Any]] "properties").asInstanceOf[Map[String, String]])
      case _ => println(s"$key is not enabled!!")
    }
  }

  /**
   * get file format
   *
   * @param sourceProperties source properties
   * @return
   */
  def getFileFormat(sourceProperties: SourceProperties): String = {
    sourceProperties.properties.contains("format") match {
      case true => sourceProperties properties "format"
      case _ => "orc"
    }
  }

  /**
   * get csv specific file read properties
   *
   * @param sourceProperties source properties
   * @return
   */
  def getCSVSpecificBatchConfigs(sourceProperties: SourceProperties): util.HashMap[String, String] = {
    val map: util.HashMap[String, String] = new util.HashMap[String, String]()
    val delimiter = sourceProperties.properties contains "delimiter" match {
      case true => sourceProperties properties "delimiter"
      case _ => ","
    }
    map.put("delimiter", delimiter)
    val header = (sourceProperties.properties contains "header") && (sourceProperties properties "header" equalsIgnoreCase "true") match {
      case true => "true"
      case _ => "false"
    }
    map.put("header", header)
    val inferSchema = (sourceProperties.properties contains "inferSchema") && (sourceProperties properties "inferSchema" equalsIgnoreCase "true") && !(sourceProperties.properties contains "schema") && !(sourceProperties.properties contains "schema_location") match {
      case true => "true"
      case _ => "false"
    }
    map.put("inferSchema", inferSchema)
    map
  }

  /**
   * read json config
   *
   * @param config config
   * @return
   */
  def readJsonConfig(config: String): Option[Any] = JSON parseFull readConfigFileAsString(config)

  /**
   * read config file as string
   *
   * @param fileName file
   * @return
   */
  def readConfigFileAsString(fileName: String): String = {
    val bufferedSource = Source fromFile fileName
    val out = bufferedSource.mkString
    bufferedSource.close
    out
  }

  /**
   * get pipeline properties from config file
   *
   * @param configFile config file
   * @param pipeline   pipeline name
   * @return
   */
  def getPipelineProperties(configFile: String, pipeline: String): (String, List[Map[String, String]], List[Map[String, String]], String, String, String) = {
    val configs = readJsonConfig(configFile)
    val mapOfPipelines: Map[String, Any] = (configs.get asInstanceOf[Map[String, Any]] "process" asInstanceOf[Map[String, Any]] pipeline)
      .asInstanceOf[Map[String, Any]]

    val customProcessFile = mapOfPipelines contains "custom_process_file" match {
      case true => mapOfPipelines("custom_process_file").asInstanceOf[String]
      case false => ""
    }
    val CustomProcessSnippet = mapOfPipelines contains "custom_process_code_snippet" match {
      case true => mapOfPipelines("custom_process_code_snippet").asInstanceOf[String]
      case false => ""
    }

    (mapOfPipelines("class").asInstanceOf[String],
      mapOfPipelines("input").asInstanceOf[List[Map[String, String]]],
      mapOfPipelines("output").asInstanceOf[List[Map[String, String]]],
      mapOfPipelines("query").asInstanceOf[String],
      customProcessFile,
      CustomProcessSnippet)
  }

  /**
   * read dataframe from source configured
   *
   * @param input_properties input properties
   * @param pipeline         pipeline name
   * @return
   */
  @throws(classOf[Exception])
  def callRead(input_properties: Map[String, String], pipeline: String): Option[(DataFrame, Option[Any])] = {
    val format = input_properties get "format"
    Try{
      format match {
        case Some("kafka") => Some(KafkaSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("mongo") => Some(MongoSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("elastic") => Some(ElasticSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("s3") => Some(S3Source read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("rdb") => Some(RDBSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("redis") => Some(RedisSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("bigquery") => Some(BigQuerySource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("rest") => Some(HttpSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("file") => Some(FileSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("sfdc") => Some(SalesForceSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("okta") => Some(OktaSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("knowBe4") => Some(KnowBe4Source read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("o365") => Some(O365Source read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case Some("snowflake") => Some(SnowFlakeSource read(SourceProperties(input_properties), process = Some(input_properties("process")), pipeline = Some(pipeline)))
        case _ => None
      }
    }
    match {
      case Success(value) => value
      case Failure(exception) => println(exception.printStackTrace())
        throw new RuntimeException()
    }

  }

  /**
   * commit kafka offset for custom streaming code
   *
   * @param list_post_process list of post process
   */
  def commitSync(list_post_process: java.util.List[(String, String, Option[Any])]): Unit = {
    list_post_process.asScala.foreach(element => {
      if (element._3.isDefined)
        KafkaHelper commitKafkaOffset(element._3.get.asInstanceOf[KafkaConsumer[Array[Byte], Array[Byte]]],
          element._1, element._2)
    })
  }

  /**
   * get query variables passed as command line args
   *
   * @param args   args
   * @param offset offset to start
   * @return
   */
  def getQueryVariablesFromArgs(args: Array[String], offset: Int): Map[String, String] = (args drop offset mkString " " split "=| " grouped 2 map { case Array(k, v) => k -> v }).toMap

  /**
   * get cols in seq from string delimited by ","
   *
   * @param partition_cols partition cols
   * @return
   */
  def getColsSeqString(partition_cols: Option[String]): Option[Seq[String]] = if (partition_cols.isDefined) Some((partition_cols.get split """,""" map (column => column.trim)).toSeq) else None

  /**
   * get read properties of redis cluster
   *
   * @param sourceProperties source properties
   * @return
   */
  def getRedisClusterReadProperties(sourceProperties: SourceProperties): mutable.HashMap[String, String] = {
    val properties = new mutable.HashMap[String, String]
    properties.put("host", sourceProperties.properties("host"))
    properties.put("port", sourceProperties.properties("port"))
    properties.put("auth", sourceProperties.properties("auth"))
    properties.put("timeout", sourceProperties.properties("timeout"))
    properties
  }

  /**
   * get write properties of redis cluster
   *
   * @param sinkProperties sink properties
   * @return
   */
  def getRedisClusterWriteProperties(sinkProperties: SinkProperties): mutable.HashMap[String, String] = {
    val configs: Map[String, String] = sinkProperties.properties
    val properties = new mutable.HashMap[String, String]
    properties.put("host", configs("host"))
    properties.put("port", configs("port"))
    properties.put("auth", configs("auth"))
    properties
  }

  /**
   * parse avs file and generate schema as structtype
   *
   * @param filename file
   * @return
   */
  def getStructTypeSchemaFromAvro(filename: String): StructType = {
    val schema: Schema = new Schema.Parser() parse readConfigFileAsString(filename)
    (SchemaConverters toSqlType schema).dataType.asInstanceOf[StructType]
  }

  /**
   * init spark streaming session
   *
   * @param properties properties
   */
  def initSparkStreamingContext(properties: Map[String, String]): Unit = {
    val spark = getSparkSession
    try {
      spark.conf.set("spark.streaming.kafka.consumer.cache.enabled", properties("consumer_cache_enabled"))
      spark.conf.set("spark.streaming.kafka.maxRatePerPartition", properties("rate_per_partition"))
      spark.conf.set("spark.streaming.backpressure.enabled", properties("backpressure_enabled"))
      ssc = new StreamingContext(spark.sparkContext, Seconds(properties("window").toInt))
    }
    catch {
      case ex: Exception =>
        logger.info(">> Exiting program. Error in initialize_spark_streaming_context(). Error: " + ex.toString)
    }
  }

  /**
   * create execution code string
   *
   * @param codeSnippet       code snippet
   * @param placeholderString place holder
   * @return
   */
  def createExecutionCodeFromSnippet(codeSnippet: String, placeholderString: String): String = {
    val customFileSource = Source fromFile "src/main/resources/customTemplate.scala"
    val fileContents = customFileSource.getLines mkString "\n"
    fileContents replace(placeholderString, codeSnippet)
  }

  /**
   * set configs for connecting to s3
   *
   * @param spark            spark session
   * @param sourceProperties source properties
   */
  def fileReadS3PreConfig(spark: SparkSession, sourceProperties: SourceProperties): Unit = {
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", sourceProperties.properties("access_key"))
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", sourceProperties.properties("secret_key"))
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark.sparkContext
      .hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  }

  /**
   * remove special character from dataframe column name
   *
   * @param df          dataframe
   * @param replaceWith replace with
   * @return
   */
  def removeSpecialCharacterFromDFColName(df: DataFrame, replaceWith: String): DataFrame = df
    .columns.foldLeft(df) { (newdf: DataFrame, colname: String) =>
    newdf.withColumnRenamed(colname, colname.replaceAll("[^a-zA-Z0-9]", replaceWith))
  }

  /**
   * get error summary
   *
   * @param logMessage
   * @return
   */
  def getErrorSummary(logMessage: LogMessage): String = {
    s"Application ${logMessage.appName} running in spark with application id ${logMessage.appId} failed with error ${logMessage.errorMessage} "
  }

  /**
   * send pagerduty alert
   *
   * @param routeKey   route key
   * @param logMessage log message
   * @return
   */
  def sendPagerDutyAlert(routeKey: String, logMessage: LogMessage): EventResult = {
    val pagerDutyEventsClient = PagerDutyEventsClient.create
    val payload = Payload.Builder
      .newBuilder
      .setSummary(getErrorSummary(logMessage))
      .setSource(logMessage.hostName)
      .setSeverity(Severity.ERROR)
      .setTimestamp(OffsetDateTime.now).build

    val incident = TriggerIncident
      .TriggerIncidentBuilder
      .newBuilder(routeKey, payload)
      .build()
    //.setDedupKey("DEDUP_KEY").build

    pagerDutyEventsClient.trigger(incident)
  }

  def decode(encodedString: String): String = {
    val decodedBytes = Base64.getDecoder.decode(encodedString)
    new String(decodedBytes)
  }

  def encode(input: String): String = {
    Base64.getEncoder.encodeToString(input.getBytes)
  }

  def writeIntermediateJSONObject(spark: SparkSession, list: java.util.List[JSONObject], location: String, delete: Boolean): Unit = {
    import spark.implicits._
    val dataset = Seq(list.toString).toDS
    val df = spark.read json dataset
    println(s"delete flag $delete")
    if (delete) {
      println("deleting data")
      deletePath(location, spark)
    }
    (df.write mode SaveMode.Append).parquet(location)
  }

  def writeIntermediateDataFrame(spark: SparkSession, dataFrame: DataFrame, location: String, tableName: String, bucket: Int, persistFormat: String, delete: Boolean): Unit = {

    val tmpPath = s"$location${File.separator}$tableName${File.separator}$bucket"
    if (delete)
      deletePath(tmpPath, spark)
    dataFrame.write.mode(SaveMode.Overwrite).format(persistFormat).save(tmpPath)
  }

  def deletePath(path: String, spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathToDelete = new Path(path)
    if (fs.exists(pathToDelete) && fs.isDirectory(pathToDelete))
      fs.delete(pathToDelete, true)
  }
}
