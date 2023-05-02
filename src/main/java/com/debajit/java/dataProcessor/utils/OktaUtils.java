package com.debajit.java.dataProcessor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.okta.sdk.authc.credentials.TokenClientCredentials;
import com.okta.sdk.client.Client;
import com.okta.sdk.client.Clients;
import com.okta.sdk.resource.log.LogEvent;
import com.okta.sdk.resource.log.LogEventList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * @author debajit
 */
public class OktaUtils {
    static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * get logs from okta and persist intermediate data with configured page pageSize
     *
     * @param url      o365 url
     * @param token    token
     * @param from     from date
     * @param until    until date
     * @param pageSize pageSize
     * @param spark    spark session
     * @param schema   schema structtype
     * @throws Exception
     */
    public static void getLogs(String url, String token, Date from, Date until,
                               int pageSize, SparkSession spark, StructType schema, String intermediatePath) throws Exception {
        List<String> list = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            Path pathToPersist = new Path(intermediatePath);
            AtomicBoolean flag = new AtomicBoolean(true);
            if (fs.exists(pathToPersist) && fs.isDirectory(pathToPersist)) {
                fs.delete(pathToPersist, true);
            }
            Client client = Clients.builder()
                    .setOrgUrl(url)
                    .setClientCredentials(new TokenClientCredentials(token))
                    .build();
            LogEventList logEvents = client.getLogs(from, until, null, null, null);
            /**
             * streaming okta logs and persisting intermediate data in parquet format as per configured page pageSize
             */
            logEvents.stream().forEach(logEvent -> {
                list.add(OktaUtils.apply(logEvent));
                if (list.size() >= pageSize) {
                    flag.set(persistOktaData(list, spark, pathToPersist, flag.get(), schema));
                    list.clear();
                }
            });
            /**
             * flush the remaining data
             */
            if (list.size() > 0) {
                flag.set(persistOktaData(list, spark, pathToPersist, flag.get(), schema));
                list.clear();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            System.out.println("End of OKTA logs fetch with url " + url + " and start -> " + from + ", end ->" + until);
        }
    }

    /**
     * persist intermediate data
     *
     * @param list          list of logs
     * @param spark         spark session
     * @param pathToPersist path to persist intermediate data
     * @param flag          overwrite flag
     * @param schema        okta schema
     * @return overwrite flag
     */
    private static boolean persistOktaData(List<String> list, SparkSession spark, Path pathToPersist, boolean flag, StructType schema) {
        System.out.println("Writing Intermediate data to " + pathToPersist + ", Number of Records: " + list.size());
        Dataset<Row> df = spark.createDataset(list, Encoders.STRING()).toDF("value")
                .select(from_json(col("value"), schema).as("data")).select("data.*");
        if (flag) {
            df.write().mode(SaveMode.Overwrite).parquet(pathToPersist.toString());
        } else df.write().mode(SaveMode.Append).parquet(pathToPersist.toString());
        return false;
    }

    /**
     * converts logs to json string
     *
     * @param logEvent log event
     * @return log in string
     */
    private static String apply(LogEvent logEvent) {
        String log;
        try {
            log = objectMapper.writeValueAsString(logEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return log;
    }
}
