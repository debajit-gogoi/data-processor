package com.debajit.java.dataProcessor.utils;

import com.debajit.dataProcessor.utils.Utils;
import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.wsdl.Constants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.json.XML;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author debajit
 */

public class SalesforceSoqlUtils {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        querySalesforceData("https://test.salesforce.com", "/services/Soap/u/52.0", "",
                "", "id, SystemModStamp", "case",
                "2022-06-22T02:12:49.000Z", "2022-06-22T02:13:49.000Z",
                2000, sparkSession, "output/salesforceSoqlData",  false, true).show();

    }

    public static Dataset<Row> querySalesforceData(String url, String path, String username, String password,
                                                   String columns, String tableName, String startTime, String endTime,
                                                   int batchSize, SparkSession spark, String directoryToPersist,
                                                   boolean forSchema, boolean delete) {
        try {
            AtomicBoolean flag = new AtomicBoolean(delete);
            System.out.println("first flag value "+flag.get());
            URI loginURI = new URI(url);
            String uri = new URI(loginURI.getScheme(), loginURI.getUserInfo(), loginURI.getHost(),
                    loginURI.getPort(), path, null, null).toString();

            ConnectorConfig config = new ConnectorConfig();
            config.setUsername(username);
            config.setPassword(password);

            config.setAuthEndpoint(uri);

            PartnerConnection partnerConnection = new PartnerConnection(config);
            // Set query batch size
            partnerConnection.setQueryOptions(batchSize);

            String columnsToQuery = columns.trim().equals("") ? "SystemModStamp" : columns;

            StringBuilder sb = new StringBuilder();

            // SOQL query to use
            sb.append("SELECT ").append(columnsToQuery).append(" FROM ").append(tableName);

            if(forSchema){
                sb.append(" LIMIT 1 ");
            } else {
                if (startTime != null && endTime == null) {
                    sb.append(" where SystemModStamp >=").append(startTime);
                }
                if (startTime != null && endTime != null) {
                    sb.append(" where SystemModStamp >=").append(startTime).append(" and SystemModStamp <").append(endTime);
                }
                if (startTime == null && endTime != null) {
                    sb.append(" where SystemModStamp <").append(endTime);
                }
            }

            System.out.println("soql is " + sb);
            // Make the query call and get the query results
            QueryResult qr = partnerConnection.query(sb.toString());
            boolean done = false;
            int loopCount = 0;
            // Loop through the batches of returned results
            List<JSONObject> jsonObjects = new ArrayList<>();
            while (!done) {
                if (loopCount > 0) {
                    flag.set(false);
                }
                SObject[] records = qr.getRecords();
                // Process the query results
                for (SObject contact : records) {
                    JSONObject jsonObject = buildJSONSObject(contact);
                    jsonObjects.add(jsonObject);
                }
                if (qr.isDone()) {
                    if (jsonObjects.size() > 0) {
                        Utils.writeIntermediateJSONObject(spark, jsonObjects, directoryToPersist, flag.get());
                        jsonObjects.clear();
                    }
                    flag.set(true);
                    done = true;
                } else {
                    if (jsonObjects.size() == 4000) {
                        Utils.writeIntermediateJSONObject(spark, jsonObjects, directoryToPersist, flag.get());
                        loopCount++;
                        jsonObjects.clear();
                    }
                    qr = partnerConnection.queryMore(qr.getQueryLocator());
                }
            }
        } catch (ConnectionException ce) {
            ce.printStackTrace();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Query execution completed.");
        return spark.read().parquet(Paths.get(directoryToPersist).toString());
    }


    public static JSONObject buildJSONSObject(SObject sobject) {
        JSONObject jsonObject = new JSONObject();
        for (XmlObject element : Objects.requireNonNull(getChildren(sobject))) {
            Object object = element.getValue();
            if (object instanceof SObject) {
                jsonObject.put(element.getName().getLocalPart(), buildJSONSObject((SObject) object));
            } else {
                jsonObject.put(element.getName().getLocalPart(), element.getValue());
            }
        }
        return jsonObject;
    }

    public static XmlObject[] getChildren(SObject object) {
        List<String> reservedFieldNames = Arrays.asList("type", "fieldsToNull");
        if (object == null) {
            return null;
        }
        List<XmlObject> children = new ArrayList<>();
        Iterator<XmlObject> iterator = object.getChildren();
        while (iterator.hasNext()) {
            XmlObject child = iterator.next();
            if (child.getName().getNamespaceURI().equals(Constants.PARTNER_SOBJECT_NS)
                    && reservedFieldNames.contains(child.getName().getLocalPart())) {
                continue;
            }
            children.add(child);
        }
        if (children.size() == 0) {
            return null;
        }
        return children.toArray(new XmlObject[children.size()]);
    }


    public static void doBulkQuery(String url, String apiVersion, String username, String password, String columns, String tableName) throws ConnectionException, AsyncApiException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(username);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(url);
        new PartnerConnection(partnerConfig);
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
                + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);
        config.setTraceMessage(false);
        BulkConnection bulkConnection = new BulkConnection(config);

        try {
            JobInfo job = new JobInfo();
            job.setObject(tableName);

            job.setOperation(OperationEnum.query);
            job.setConcurrencyMode(ConcurrencyMode.Parallel);
            job.setContentType(ContentType.XML);

            job = bulkConnection.createJob(job);
            assert job.getId() != null;

            job = bulkConnection.getJobStatus(job.getId());

            String query =
                    "SELECT " + columns + " FROM " + tableName;

            BatchInfo info = null;
            ByteArrayInputStream bout =
                    new ByteArrayInputStream(query.getBytes());
            info = bulkConnection.createBatchFromStream(job, bout);

            String[] queryResults = null;
            for (int i = 0; i < 10000; i++) {
                queryResults = null;
                Thread.sleep(30000); //30 sec
                info = bulkConnection.getBatchInfo(job.getId(),
                        info.getId());

                if (info.getState() == BatchStateEnum.Completed) {
                    QueryResultList list =
                            bulkConnection.getQueryResultList(job.getId(),
                                    info.getId());
                    queryResults = list.getResult();
                    break;
                } else if (info.getState() == BatchStateEnum.Failed) {
                    System.out.println("-------------- failed ----------"
                            + info);
                    break;
                } else {
                    System.out.println("-------------- waiting ----------"
                            + info);
                }
            }

            if (queryResults != null) {
                List<JSONObject> jsonObjects = new ArrayList<>();
                for (String resultId : queryResults) {
                    InputStream inputStream = bulkConnection.getQueryResultStream(job.getId(),
                            info.getId(), resultId);

                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        JSONObject xmlJSONObj = XML.toJSONObject(line);
                        jsonObjects.add(xmlJSONObj);
                        System.out.println(xmlJSONObj);
                    }
                }

                // Write jsonObjects to hdfs
            }
        } catch (AsyncApiException | InterruptedException aae) {
            aae.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
