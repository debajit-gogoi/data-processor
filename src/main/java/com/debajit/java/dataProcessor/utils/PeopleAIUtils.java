package com.debajit.java.dataProcessor.utils;

import com.debajit.dataProcessor.spark.SparkHelper;
import com.debajit.dataProcessor.utils.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.javatuples.Pair;
import org.json.JSONObject;
import scala.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;


/**
 * @author debajit
 */

public class PeopleAIUtils {

    public static void main(String[] args) throws IOException, InterruptedException {

        SparkSession sparkSession = SparkHelper.getAndConfigureSparkSession(Option.apply("local[*]"),
                Option.apply("Test"));
        StructType schema = Utils.getStructTypeSchemaFromAvro("src/main/resources/schema/avsc/peopleai.avsc");
        getPeopleAIDate(sparkSession, "https://api.people.ai", "", "", "output/peopleai", "2022-08-22", "2022-08-22", "Delta", 10000, schema);
    }

    public static void getPeopleAIDate(SparkSession spark, String url, String clientId, String clientSecret, String tmpPath, String startDate, String endDate, String exportType, int batchSize, StructType schema) throws InterruptedException {
        String urlParameters = "client_id=" + clientId + "&client_secret=" + clientSecret;
        String accessToken = HTTPUtils.callHTTPPostMethod(urlParameters, url + "/auth/v1/tokens", "application/x-www-form-urlencoded", Optional.ofNullable(null)).getString("access_token");
        System.out.println(accessToken);

        String jsonInputString = "{\"start_date\": \"" + startDate + "\", \"end_date\": \"" + endDate + "\",\"activity_type\": \"all\",\"export_type\": \"" + exportType + "\",\n" +
                "\"output_format\": \"JSONLines\"}";
        String jobId = HTTPUtils.callHTTPPostMethod(jsonInputString, url + "/pull/v1/export/activities/jobs", "application/json", Optional.ofNullable(accessToken)).getString("job_id");
        System.out.println(jobId);

        boolean flag = true;

        while (flag) {
            String state = HTTPUtils.callHTTPGetMethod(url + "/pull/v1/export/activities/jobs/" + jobId, "application/json", Optional.ofNullable(accessToken)).getString("state");
            System.out.println(state);
            if (state.equals("Running") || state.equals("Queued")) Thread.sleep(10000);
            else flag = false;
        }
        List<String> jsonObjectList = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            Path pathToPersist = new Path(tmpPath);
            AtomicBoolean toDelete = new AtomicBoolean(true);
            if (fs.exists(pathToPersist) && fs.isDirectory(pathToPersist)) {
                fs.delete(pathToPersist, true);
            }
            Pair<BufferedReader, HttpURLConnection> pair = HTTPUtils.getHTTPGetReader(url + "/pull/v1/export/activities/jobs/" + jobId + "/data", "application/json", Optional.ofNullable(accessToken));
            String responseLine;
            while ((responseLine = pair.getValue0().readLine()) != null) {
                JSONObject jsonObject = new JSONObject(responseLine.trim());

                jsonObjectList.add(jsonObject.toString());

                if (jsonObjectList.size() >= batchSize) {
                    toDelete.set(persistPeopleaiData(jsonObjectList, spark, pathToPersist, toDelete.get(),schema));
                    jsonObjectList.clear();
                }
            }
            /**
             * flush the remaining data
             */
            if (jsonObjectList.size() > 0) {
                toDelete.set(persistPeopleaiData(jsonObjectList, spark, pathToPersist, toDelete.get(),schema));
                jsonObjectList.clear();
            }
            pair.getValue0().close();
            pair.getValue1().disconnect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static boolean persistPeopleaiData(List<String> list, SparkSession spark, Path pathToPersist, boolean flag, StructType schema) {
        System.out.println("Writing Intermediate data to " + pathToPersist + ", Number of Records: " + list.size());
        Dataset<Row> df = spark.createDataset(list, Encoders.STRING()).toDF("value")
                .select(from_json(col("value"), schema).as("data")).select("data.*");
        df.show(false);
        df.printSchema();
        if (flag) {
            df.write().mode(SaveMode.Overwrite).parquet(pathToPersist.toString());
        }
        df.write().mode(SaveMode.Append).parquet(pathToPersist.toString());
        return false;
    }
}
