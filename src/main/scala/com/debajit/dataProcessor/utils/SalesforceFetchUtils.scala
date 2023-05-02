package com.debajit.dataProcessor.utils

import com.debajit.dataProcessor.spark.SparkHelper
import com.debajit.java.dataProcessor.utils.SalesforceSoqlUtils
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, TimeZone}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
 * @author debajit,debajit
 */

object SalesforceFetchUtils {
  val SystemModStampTimeFormatJDBC = "yyyy-MM-dd HH:mm:ss"
  val SystemModStampTimeFormatSoql = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  val formatJDBC = new SimpleDateFormat(SystemModStampTimeFormatJDBC)
  val formatSoql = new SimpleDateFormat(SystemModStampTimeFormatSoql)
  private val BUCKET = "bucket"
  private val ZONE_ID = TimeZone.getDefault.getID

  def main(args: Array[String]): Unit = {

    println(convertTimeZoneToSystemJDBC("2021-08-28 20:51:23"))
    println(convertDateInDiffFormat("2022-06-30 15:11:14", formatJDBC, formatSoql))

    val spark = SparkHelper.getAndConfigureSparkSession(Some("local[*]"), Some("Test"))
    spark.conf set("spark.sql.session.timeZone", "UTC")
    val properties = Map("tmp_storage" -> "output/salesforcetest",
      "buffer_size" -> "20000",
      "parallelism" -> "4",
      "table" -> "campaignmember",
      "user" -> "",
      "password" -> "",
      "token" -> "",
      "url" -> "https://test.salesforce.com",
      "columns" -> "id,isdeleted,campaignid,leadid,contactid,status,hasresponded,createddate,createdbyid,lastmodifieddate,lastmodifiedbyid,systemmodstamp,firstrespondeddate,salutation,name,firstname,lastname,title,street,city,state,postalcode,country,email,phone,fax,mobilephone,description,donotcall,hasoptedoutofemail,hasoptedoutoffax,leadsource,companyoraccount,type,leadorcontactid,leadorcontactownerid,net_new__c,cfcr_infer_fit_score_1_100_on_create__c,reporting_matched_account_open_pipe__c,partner_status__c,community_invite_code__c,sso_status__c,lead_contact_status__c,t_shirt_size__c,annual_revenue_range__c,last_checkin__c,hardware_model__c,type_of_deployment__c,number_of_nodes__c,converted_to_opp__c,tracking_number__c,fcrm__fcr_18_character_id__c,utm_source2__c,total_amount_of_sales__c,g2000_ranking__c,fcrm__fcr_admin_opportunity_status__c,fcrm__fcr_admin_response_control__c,fcrm__fcr_admin_response_day__c,fcrm__fcr_admin_syncpending__c,fcrm__fcr_admin_synctest__c,fcrm__fcr_admin_update_counter__c,fcrm__fcr_campaign_repeat_parent__c,fcrm__fcr_cascadeid__c,fcrm__fcr_closedoprevenuemodel1__c,fcrm__fcr_closedoprevenuemodel2__c,fcrm__fcr_closedoprevenuemodel3__c,fcrm__fcr_converted_lead__c,fcrm__fcr_current_lead_contact_status__c,fcrm__fcr_dated_opportunity_amount__c,fcrm__fcr_fccrm_logo__c,fcrm__fcr_first_owner_assigned__c,fcrm__fcr_first_owner_type__c,fcrm__fcr_first_owner_worked__c,fcrm__fcr_first_queue_assigned__c,fcrm__fcr_inquiry_target_date__c,fcrm__fcr_inquiry_target__c,fcrm__fcr_inquiry_to_mqr__c,fcrm__fcr_last_modified_by_date_formula__c,fcrm__fcr_last_modified_by_formula__c,fcrm__fcr_lead_contact_18_digit_id__c,fcrm__fcr_lostoprevenuemodel1__c,fcrm__fcr_lostoprevenuemodel2__c,fcrm__fcr_lostoprevenuemodel3__c,fcrm__fcr_mqr_to_sar__c,fcrm__fcr_member_type_on_create__c,fcrm__fcr_name_created_date__c,fcrm__fcr_non_response_audit__c,fcrm__fcr_nurture_timeout__c,fcrm__fcr_openoprevenuemodel1__c,fcrm__fcr_openoprevenuemodel2__c,fcrm__fcr_openoprevenuemodel3__c,fcrm__fcr_opportunity_amount__c,fcrm__fcr_opportunity_cleared__c,fcrm__fcr_opportunity_closed_date__c,fcrm__fcr_opportunity_closed_won__c,fcrm__fcr_opportunity_closed__c,fcrm__fcr_opportunity_count__c,fcrm__fcr_opportunity_create_date__c,fcrm__fcr_opportunity_created_by__c,fcrm__fcr_opportunity_response_error__c,fcrm__fcr_opportunity_value_lost__c,fcrm__fcr_opportunity_value_won__c,fcrm__fcr_opportunity__c,fcrm__fcr_original_campaign__c,fcrm__fcr_precedence_campaign__c,fcrm__fcr_precedence_replaced_date__c,fcrm__fcr_precedence_response_link__c,fcrm__fcr_precedence_response__c,fcrm__fcr_qr_date__c,fcrm__fcr_qr__c,fcrm__fcr_repeat_count__c,fcrm__fcr_replaced_campaign__c,fcrm__fcr_replaced_response_link__c,fcrm__fcr_replaced_response__c,fcrm__fcr_response_date__c,fcrm__fcr_response_engagement_level__c,fcrm__fcr_response_score__c,fcrm__fcr_response_status__c,fcrm__fcr_revenue_timestamp__c,fcrm__fcr_sar_date__c,fcrm__fcr_sar_owner__c,fcrm__fcr_sar__c,fcrm__fcr_sar_to_sqr__c,fcrm__fcr_sqr_date__c,fcrm__fcr_sqr_won__c,fcrm__fcr_sqr__c,fcrm__fcr_sqr_to_closed_won__c,fcrm__fcr_status_age__c,fcrm__fcr_status_last_set__c,fcrm__fcr_superpower_field__c,fcrm__fcr_tqr__c,fcrm__fcr_totaloprevenuemodel1__c,fcrm__fcr_totaloprevenuemodel2__c,fcrm__fcr_totaloprevenuemodel3__c,fcrm__fcr_view_response__c,fcrm__opportunity_value_open__c,cfcr_actionable__c,cfcr_admin_defective_member__c,cfcr_count__c,cfcr_created_by_formula__c,cfcr_current_stage_in_funnel_date__c,cfcr_current_stage_in_funnel__c,cfcr_fccrm_threshold_current__c,cfcr_fccrm_threshold_on_create__c,cfcr_inquiry_to_close_wd__c,cfcr_inquiry_to_mql_wd__c,cfcr_last_interesting_moment_date__c,cfcr_last_interesting_moment_desc__c,cfcr_last_interesting_moment_source__c,cfcr_last_interesting_moment_type__c,cfcr_last_interesting_moment__c,cfcr_lead_score_on_create__c,cfcr_lead_score_on_update__c,cfcr_lead_source_formula__c,cfcr_mql_to_sal_wd__c,cfcr_manual_adjustment_notes__c,cfcr_manual_adjustment__c,cfcr_opportunity_18_digit_id__c,cfcr_opportunity_stage__c,cfcr_opportunity_type__c,cfcr_original_referrer__c,cfcr_original_search_engine__c,cfcr_original_search_phrase__c,cfcr_original_source_type__c,cfcr_sal_to_sql_wd__c,cfcr_sql_to_closed_won_wd__c,cfcr_srl_date__c,cfcr_srl__c,cfcr_still_in_inquiry__c,cfcr_still_in_mql__c,cfcr_still_in_sal__c,cfcr_still_in_sql__c,converted_date__c,country__c,lattice_lead_grade_on_create__c,lattice_lead_grade_on_update__c,lattice_lead_score_on_create__c,lattice_lead_score_on_update__c,lead_score_current__c,meeting_date__c,region_sub_region__c,sal_to_sql__c,sql_date__c,sql__c,sql_to_won__c,segment__c,status_detail__c,still_in_sql__c,theater__c,utm_campaign__c,utm_medium__c,utm_source__c,utm_term__c,matched_account_industry__c,annual_revenue__c,membercreateddate__c,tt_itda_watch__tech_target_import_timestamp__c,contact_lead_mops_notes__c,cfcr_infer_fit_score_1_100_on_update__c,sales_rep__c,sales_rep_address__c,cfcr_infer_fit_score_on_create__c,cfcr_infer_fit_score_on_update__c,cfcr_infer_score_snapshot__c,additional_notes__c,xgapp_child__c,xgapp_parent__c,event_promotion_code__c,attendee_type__c,registration_type__c,pernix_record_id__c,utm_keyword__c,fcrm__fcr_reactivation_date__c,member_theater__c,member_region__c,member_subregion__c,member_account_type__c,promo_type__c,promo_owner__c,customer_promo_eligible__c,number_of_employees_global__c,ebc_ww_target_account__c,siftrock_reply_date__c,siftrock_reply_text__c,account_type__c,lead_contact_account_partner_status__c,campemail__c,dq_checkbox__c,promo_types__c,matched_account_name__c,matched_account_id__c,utm_source_mops__c,number_of_active_opportunities__c,number_of_active_deal_reg__c,total_amount_of_sales_in_hierarchy__c,op_job_level__c,op_job_function__c,op_persona__c,op_it_job_sub_function__c,utm_search_term__c,member_account_type_on_create__c,conversion_optimized__c,lead_contact_status_picklist__c,fy18_xgap_classification__c,configurations__c,shipping_address__c,shipping_city__c,shipping_country__c,shipping_postal_code__c,shipping_state_province__c,sales_rep_email__c,sendoso_sales_rep_email__c,sendoso_sales_rep__c,sendoso_send_date__c,shipping_address_2__c,partner_rep_email__c,partner_sales_rep__c,next_alumni__c,marketing_campaign_member_type__c,mql_checkbox_date__c,mql_checkbox__c,passed_to_inside_sales_date__c,passed_to_inside_sales__c,account_segmentation__c,named_vs_non_named__c,assigned_owner_stamp__c,assigned_owner_job_role_stamp__c,lead_contact_owner_job_role__c,mtn_date__c,funnel_type__c,funnel_follow_up_status__c,lead_contact_status_upon_response__c,passed_to_outside_sales_date__c,active_funnel_lifespan__c,mtn__c,still_in_outside_sales__c,passed_to_outside_sales__c,current_active_funnel__c,track_date_time_for_report__c,marketing_initiative_notes__c,campaign_type__c,matched_account_owner_name__c,marketing_lead_contact_type__c,beam_account_state__c,beam_activation__c,campaign_member_account_type_stamp__c,lead_contact_behavior_score__c,lead_contact_firmographic_score__c,lead_contact_fit_score__c,leandata_processing__c,lead_contact_overall_score__c,most_recent_qualifying_campaign__c,mql_checkbox_datetime__c,named_account_type__c,pis_datetime__c,prospecting_datetime__c,sal_meeting_datetime__c,cfcr_sql_datetime__c,total_number_of_touches_since_open__c,won_datetime__c,backpopulated_sal__c,currencyisocode,partner_account__c,tenant_name__c,update_xi_campaign_status__c,xi_activation__c,xi_request_update__c,cell_fqdn__c,cfcr_meets_mql_definition__c,mql_date_set__c,prospecting_date_set__c,sal_date_set__c,utm_experience__c,hardware_type__c,service_type__c,tenanttype__c,explicit_opt_in__c,pis_fiscal_week__c,pis_qtr_year__c,prospecting_fiscal_week__c,prospecting_qtr_year__c,sal_fiscal_week__c,sal_qtr_year__c,actively_being_sequenced__c,name_of_currently_active_sequence__c,fiscal_week__c,current_lead_contact_status_detail__c,test_drive_status__c,cfcr_mel__c,cfcr_mel_date__c,positive_meeting__c,opp_created__c,cm_behavior_score__c,opportunity__c",
      "type_of_extraction" -> "soql",
      "tmp_write_format" -> "parquet")
    //, "condition" -> "(net_new__c = TRUE or hasresponded = TRUE or mql_checkbox__c = TRUE )")

    fetchSFUtil(spark, properties, Some("2022-07-01T00:00:00Z"), None)

    val df = spark.read.parquet("output/salesforcetest/campaignmember/*")
    df.show(false)
    df.printSchema()
    val sinkProperties = Map(
      "url" -> "jdbc:postgresql://drt-ds-dev-cluster.corp.debajit.com:5432/ntnx_ds",
      "driver" -> "org.postgresql.Driver",
      "user" -> "salesforce",
      "password" -> "",
      "database" -> "ntnx_ds",
      "tablename" -> "sfdc.campaignmember_sparktest_1",
      "operation" -> "insert",
      "cast_to_table_schema" -> "true"
    )
    //RDBSink.write(df, SinkProperties(sinkProperties))
  }

  def convertDateInDiffFormat(dateStr: String, fromFormat: SimpleDateFormat, toFormat: SimpleDateFormat): String = toFormat.format(fromFormat.parse(dateStr))

  def convertTimeZoneToSystemJDBC(dateStr: String): String = {
    this.synchronized {
      val calendar = Calendar.getInstance
      calendar setTime (formatJDBC parse dateStr)
      calendar add(Calendar.MILLISECOND, (TimeZone getTimeZone ZONE_ID).getRawOffset)
      formatJDBC format calendar.getTime
    }
  }

  def adjustEndTime(format: SimpleDateFormat, dateStr: String): String = {
    val calendar = Calendar.getInstance
    calendar setTime (format parse dateStr)
    calendar add(Calendar.SECOND, 1)
    format format calendar.getTime
  }

  def sFGetWindowPartitionMetaData(spark: SparkSession, userName: String, password: String, securityToken: String, loginUrl: String, tableName: String,
                                   startTime: Option[String], endTime: Option[String], condition: Option[String], bufferSize: Long,
                                   typeOfExtraction: String, pathToPersist: String): DataFrame = {
    val dfForBucket = typeOfExtraction match {
      case "cdata" => querySalesforceJDBC(spark, tableName, None, None,
        getJDBCUrl(userName, password, securityToken, loginUrl),
        startTime, endTime, condition, false)
      case "soql" => SalesforceSoqlUtils.querySalesforceData(
        loginUrl,
        "/services/Soap/u/52.0",
        userName,
        s"$password$securityToken",
        "",
        tableName,
        startTime.get,
        if (endTime.isDefined) endTime.get else null,
        2000,
        spark,
        pathToPersist,
        false,
        true)
    }

    //2022-06-04 00:00:00
    //yyyy-MM-dd'T'HH:mm:ss'Z'
    val dataDf = dfForBucket
      .withColumn("day", substring(col("SystemModStamp"), 1, 10))
      .withColumn("hour", substring(col("SystemModStamp"), 12, 2))
      .withColumn("minute", substring(col("SystemModStamp"), 15, 2))
      .withColumn("seconds", substring(col("SystemModStamp"), 18, 2))
      .groupBy("day", "hour", "minute", "seconds")
      .count().withColumn("bufferSize", lit(bufferSize))
    val windowSpec = Window.partitionBy("bufferSize").orderBy("day", "hour", "minute", "seconds")
    dataDf.withColumn("cumSum", sum(col("count")).over(windowSpec))
      .withColumn(BUCKET, col("cumSum") / col("bufferSize") cast IntegerType)
  }

  def fetchSFUtilJDBC(spark: SparkSession, properties: Map[String, String], startTime: Option[String], endTime: Option[String]): DataFrame = {
    spark.conf set("spark.sql.session.timeZone", "UTC")

    val condition = properties.get("condition")

    val countToExecute = querySalesforceJDBC(spark, properties("table"), Some("count(id) as count"), None,
      getJDBCUrl(properties("user"), properties("password"), properties("token"), properties("url")),
      startTime, endTime, condition, false)
      .select(col("count")).first().getLong(0)
    println(s"records to fetch count = $countToExecute")
    countToExecute > properties("buffer_size").toLong match {
      case true =>
        println(s"Retrieving through parallel bulk fetch, retrieval size is more than buffer size ${properties("buffer_size")}")
        bulkFetch(spark, properties("type_of_extraction"), properties("columns"), properties("table"),
          properties("user"), properties("password"), properties("token"), properties("url"), startTime, endTime, properties("buffer_size").toLong,
          properties("parallelism").toInt, properties("tmp_storage"), properties("tmp_write_format"), condition)
      case false =>
        println(s"Querying salesforce directly, retrieval size less than buffer size ${properties("buffer_size")}")
        querySalesforceJDBC(spark, properties("table"), properties.get("columns"), None,
          getJDBCUrl(properties("user"), properties("password"), properties("token"), properties("url")),
          startTime, endTime, condition, false)
    }
  }

  def fetchSFUtilSoql(spark: SparkSession, properties: Map[String, String], startTime: Option[String], endTime: Option[String]): DataFrame = {
    /**
     * TODO : put condition filter
     */
    val condition = properties.get("condition")

    val isBulkFetch =  properties.contains("bulk") && properties("bulk").equals("true") match {
      case true => true
      case _ => false
    }

    if (!isBulkFetch) {
      val countToExecute = SalesforceSoqlUtils.querySalesforceData(
        properties("url"),
        "/services/Soap/u/52.0",
        properties("user"),
        s"${properties("password")}${properties("token")}",
        "COUNT(Id) countData",
        properties("table"),
        startTime.get,
        if (endTime.isDefined) endTime.get else null,
        2000,
        spark,
        properties("tmp_storage"),
        false,
        true)
        .select(col("countData")).first().getLong(0)
      println(s"deleting tmp data from ${properties("tmp_storage")} after count!!")

      Utils.deletePath(properties("tmp_storage"), spark)
      println(s"records to fetch count = $countToExecute")

      countToExecute > properties("buffer_size").toLong match {
        case true =>
          println(s"Retrieving through parallel bulk fetch, retrieval size is more than buffer size ${properties("buffer_size")}")
          bulkFetch(spark, properties("type_of_extraction"), properties("columns"), properties("table"),
            properties("user"), properties("password"), properties("token"), properties("url"), startTime, endTime, properties("buffer_size").toLong,
            properties("parallelism").toInt, properties("tmp_storage"), properties("tmp_write_format"), condition)
        case false =>
          println(s"Querying salesforce directly, retrieval size less than buffer size ${properties("buffer_size")}")
          SalesforceSoqlUtils.querySalesforceData(
            properties("url"),
            "/services/Soap/u/52.0",
            properties("user"),
            s"${properties("password")}${properties("token")}",
            properties("columns"),
            properties("table"),
            startTime.get,
            if (endTime.isDefined) endTime.get else null,
            2000,
            spark,
            properties("tmp_storage"),
            false,
            true)
      }
    }
      else
      bulkFetch(spark, properties("type_of_extraction"), properties("columns"), properties("table"),
        properties("user"), properties("password"), properties("token"), properties("url"), startTime, endTime, properties("buffer_size").toLong,
        properties("parallelism").toInt, properties("tmp_storage"), properties("tmp_write_format"), condition)
  }

  def fetchSFUtil(spark: SparkSession, properties: Map[String, String], startTime: Option[String], endTime: Option[String]): DataFrame = {
    Utils.deletePath(s"${properties("tmp_storage")}${File.separator}${properties("table")}", spark)
    val typeOfExtraction: String = properties("type_of_extraction")
    typeOfExtraction match {
      case "soql" => fetchSFUtilSoql(spark, properties, startTime, endTime)
      case "cdata" => fetchSFUtilJDBC(spark, properties, startTime, endTime)
    }
  }

  def bulkFetch(spark: SparkSession, typeOfExtraction: String, columns: String, tableName: String, userName: String,
                password: String, securityToken: String, loginUrl: String, startTime: Option[String], endTime: Option[String],
                bufferSize: Long, parallelism: Int, pathToPersist: String, persistFormat: String, condition: Option[String]): DataFrame = {
    if (typeOfExtraction.equals("cdata"))
      spark.conf set("spark.sql.session.timeZone", "UTC")

    val dfBucket = sFGetWindowPartitionMetaData(
      spark, userName, password, securityToken, loginUrl, tableName, startTime, endTime,
      condition, bufferSize, typeOfExtraction, pathToPersist)

    val windowSpecAgg: WindowSpec = Window partitionBy BUCKET
    val colsDfBucket: mutable.ListBuffer[String] = dfBucket.columns.to[ListBuffer]
    colsDfBucket -= BUCKET
    val aggregatedDf = applyWindowFunc(dfBucket, windowSpecAgg, colsDfBucket)
    val colsAggregatedDf: mutable.ListBuffer[String] = aggregatedDf.columns.to[ListBuffer]
    colsAggregatedDf -= BUCKET
    val windowDf = typeOfExtraction match {
      case "soql" => getDFWithStartEndDateSoql(aggregatedDf, colsAggregatedDf)
      case _ => getDFWithStartEndDateJDBC(aggregatedDf, colsAggregatedDf)
    }

    val forkJoinPool = new ForkJoinPool(parallelism)

    val failedBucket = new util.ArrayList[Bucket]()

    var bucketExtract: mutable.ListBuffer[Bucket] = (windowDf collect()).toList.map(row => Bucket(
      bucket = row getInt (row fieldIndex "bucket"),
      startTime = row getString (row fieldIndex "startDate"),
      endTime = row getString (row fieldIndex "endDate")
    )).sortBy(bucket => bucket.bucket).sliding(2).map {
      case List(a, b) => Bucket(a.bucket, a.startTime, b.startTime)
      case List(a) => Bucket(a.bucket, a.startTime, a.endTime)
    }.to[ListBuffer]

    val lastBucket: Bucket = endTime.isDefined match {
      case true => Bucket(bucketExtract.last.bucket + 1, bucketExtract.last.endTime, endTime.get)
      case false => Bucket(bucketExtract.last.bucket + 1, bucketExtract.last.endTime, null)
    }
    //val lastBucket: Bucket = Bucket(bucketExtract.last.bucket + 1, bucketExtract.last.endTime, null)
    bucketExtract += lastBucket

    println(s"deleting tmp data from $pathToPersist after bucketing!!")
    Utils.deletePath(pathToPersist,spark)

    println("Computed buckets are listed below:-")
    bucketExtract.foreach(println)

    //System.exit(0)
    val executionList: ParSeq[Bucket] = bucketExtract.par
    executionList.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
    executionList foreach (bucket => {
      Try {
        typeOfExtraction match {
          case "soql" =>
            SalesforceSoqlUtils querySalesforceData(
              loginUrl,
              "/services/Soap/u/52.0",
              userName,
              s"$password$securityToken",
              columns,
              tableName,
              bucket.startTime,
              if (bucket.endTime != null) bucket.endTime else null,
              2000,
              SparkHelper.getSparkSession,
              s"$pathToPersist${File.separator}$tableName${File.separator}${bucket.bucket}",
              false,
              true);
          case _ =>
            bucket.endTime != null match {
              case true => querySalesforceJDBC(spark, tableName, Some(columns), None, getJDBCUrl(userName, password, securityToken, loginUrl),
                Some(bucket.startTime), Some(bucket.endTime), condition, forSchema = false)
              case false => querySalesforceJDBC(spark, tableName, Some(columns), None, getJDBCUrl(userName, password, securityToken, loginUrl),
                Some(bucket.startTime), None, condition, forSchema = false)
            }
        }
      }
      match {
        case Success(value) =>
          if(typeOfExtraction.equals("cdata"))
            Utils writeIntermediateDataFrame(spark, value, pathToPersist, tableName, bucket.bucket, persistFormat, false)
        case Failure(exception) =>
          println(exception.printStackTrace())
          println(s"Adding bucket $bucket for to list of failed attempts to retry later!!!")
          failedBucket.add(bucket)
      }
    })

    if (failedBucket.size() > 0) {
      println("Retrying the failed attempts!!")
    }

    import scala.collection.JavaConverters._
    failedBucket.asScala.foreach(bucket => {
      Try {
        typeOfExtraction match {
          case "soql" => SalesforceSoqlUtils querySalesforceData(
            loginUrl,
            "/services/Soap/u/52.0",
            userName,
            s"$password$securityToken",
            columns,
            tableName,
            bucket.startTime,
            if (bucket.endTime != null) bucket.endTime else null,
            2000,
            SparkHelper.getSparkSession,
            s"$pathToPersist${File.separator}$tableName${File.separator}${bucket.bucket}",
            false,
            true);
          case "cdata" => querySalesforceJDBC(spark, tableName, Some(columns), None, getJDBCUrl(userName, password, securityToken, loginUrl),
            Some(bucket.startTime), Some(bucket.endTime), condition, forSchema = false)
        }
      }
      match {
        case Success(value) =>
          if(typeOfExtraction.equals("cdata"))
            Utils writeIntermediateDataFrame(spark, value, pathToPersist, tableName, bucket.bucket, persistFormat, false)
        case Failure(exception) =>
          println(exception.printStackTrace())
          println(s"Extract for bucket ${bucket.bucket} failed, \nStar time: ${bucket.startTime}, End time: ${bucket.endTime}")
          println("Bulk fetch failed!!")
          System.exit(1)
      }
    })
    spark.read.format(persistFormat).load(s"$pathToPersist${File.separator}$tableName${File.separator}*")
  }

  def getDFWithStartEndDateSoql(aggregatedDf: DataFrame, colsAggregatedDf: mutable.Seq[String]): DataFrame = aggregatedDf
    .withColumn("startDate", concat(col("startDay"), lit("T"), col("startHour"),
      lit(":"), col("startMinute"), lit(":"), col("startSecond"), lit("Z")))
    .withColumn("endDate", concat(col("endDay"), lit("T"), col("endHour"),
      lit(":"), col("endMinute"), lit(":"), col("endSecond"), lit("Z")))
    .drop(colsAggregatedDf: _*)

  def getDFWithStartEndDateJDBC(aggregatedDf: DataFrame, colsAggregatedDf: mutable.Seq[String]): DataFrame = aggregatedDf
    .withColumn("startDate", concat(col("startDay"), lit(" "), col("startHour"),
      lit(":"), col("startMinute"), lit(":"), col("startSecond")))
    .withColumn("endDate", concat(col("endDay"), lit(" "), col("endHour"),
      lit(":"), col("endMinute"), lit(":"), col("endSecond")))
    .drop(colsAggregatedDf: _*)

  def applyWindowFunc(dfBucket: DataFrame, windowSpecAgg: WindowSpec, colsDfBucket: mutable.ListBuffer[String]): Dataset[Row] = {
    dfBucket
      .withColumn("startDay", first(col("day")) over windowSpecAgg)
      .withColumn("endDay", last(col("day")) over windowSpecAgg)
      .withColumn("startHour", first(col("hour")) over windowSpecAgg)
      .withColumn("endHour", last(col("hour")) over windowSpecAgg)
      .withColumn("startMinute", first(col("minute")) over windowSpecAgg)
      .withColumn("endMinute", last(col("minute")) over windowSpecAgg)
      .withColumn("startSecond", first(col("seconds")) over windowSpecAgg)
      .withColumn("endSecond", last(col("seconds")) over windowSpecAgg)
      .drop(colsDfBucket: _*)
      .distinct()
      .orderBy(BUCKET)
  }

  @throws(classOf[Exception])
  def querySalesforceJDBC(spark: SparkSession, tableName: String, columns: Option[String],
                          query: Option[String], jdbcURL: String,
                          startTime: Option[String], endTime: Option[String], condition: Option[String],
                          forSchema: Boolean): DataFrame = {
    spark.conf set("spark.sql.session.timeZone", "UTC")
    val cols = columns.isDefined match {
      case true => columns.get
      case false => "SystemModStamp"
    }

    val queryToExecute: String = query.isDefined match {
      case true => query.get
      case false => buildQuery(cols, tableName, startTime, endTime, condition)
    }

    try {
      println(s"Executing query : $queryToExecute; ")
      getData(spark, jdbcURL, forSchema, queryToExecute)
    }
    catch {
      case ex: Exception => println(ex.printStackTrace())
        try {
          println(s"Re-executing query : $queryToExecute; ")
          getData(spark, jdbcURL, forSchema, queryToExecute)
        }
        catch {
          case ex: Exception => println(ex.printStackTrace())
            throw new RuntimeException()
        }
    }
  }

  @throws(classOf[Exception])
  def getData(spark: SparkSession, jdbcURL: String, forSchema: Boolean, queryToExecute: String): DataFrame = {
    Try {
      Class.forName("cdata.jdbc.salesforce.SalesforceDriver")
      spark.read.format("jdbc")
        .option("url", jdbcURL)
        .option("driver", "cdata.jdbc.salesforce.SalesforceDriver")
        .option("query", if (forSchema) queryToExecute.concat(" limit 1") else queryToExecute)
        .load()
    }
    match {
      case Success(value) => value
      case Failure(ex) => ex.printStackTrace()
        throw ex
    }

  }

  def getJDBCUrl(userName: String, password: String, securityToken: String, loginUrl: String): String = {
    s"jdbc:salesforce:User=$userName;Password=$password;Security Token=$securityToken;LoginURL=$loginUrl;RTK=52465247565A30373235323233305745425452314131000000000000000000000000000000000000313131313131313100005742375A3647463533474E580000;"
  }

  def buildQuery(cols: String, tableName: String, startTime: Option[String], endTime: Option[String], condition: Option[String]): String = {
    val queryBuilder = new mutable.StringBuilder(s"Select $cols from `$tableName`")
    startTime.isDefined && endTime.isDefined match {
      case true => queryBuilder append s" where SystemModStamp >='${convertTimeZoneToSystemJDBC(startTime.get)}' and SystemModStamp <'${convertTimeZoneToSystemJDBC(endTime.get)}'"
      case false => startTime.isDefined match {
        case true => queryBuilder append s" where SystemModStamp >='${convertTimeZoneToSystemJDBC(startTime.get)}'"
        case false => endTime.isDefined match {
          case true => queryBuilder append s" where SystemModStamp <'${convertTimeZoneToSystemJDBC(endTime.get)}'"
          case false =>
        }
      }
    }

    condition.isDefined match {
      case true => queryBuilder.toString().contains(" where ") match {
        case true => queryBuilder.append(s" and ${condition.get}")
        case false => queryBuilder.append(s" where ${condition.get}")
      }
      case false =>
    }

    queryBuilder toString()
  }
}

case class Bucket(bucket: Int, startTime: String, endTime: String) {
  override def productPrefix = "Bucket : "
}
