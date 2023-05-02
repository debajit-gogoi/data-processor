package com.debajit.java.dataProcessor.utils;


import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.json.JSONObject;
import org.json.XML;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author debajit
 */

public class SalesforceBulkSoap {

    public static void main(String[] args) throws ConnectionException, AsyncApiException, IOException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername("**");
        partnerConfig.setPassword("**");
        partnerConfig.setAuthEndpoint("https://test.salesforce.com/services/Soap/u/52.0");
        // Creating the connection automatically handles login and stores
        // the session in partnerConfig
        new PartnerConnection(partnerConfig);
        // When PartnerConnection is instantiated, a login is implicitly
        // executed and, if successful,
        // a valid session is stored in the ConnectorConfig instance.
        // Use this key to initialize a BulkConnection:
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        // The endpoint for the Bulk API service is the same as for the normal
        // SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String apiVersion = "55.0";
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))
                + "async/" + apiVersion;
        config.setRestEndpoint(restEndpoint);
        // This should only be false when doing debugging.
        config.setCompression(true);
        // Set this to true to see HTTP requests and responses on stdout
        config.setTraceMessage(false);
        BulkConnection connection = new BulkConnection(config);

        doBulkQuery(connection);
    }


    public static void doBulkQuery(BulkConnection bulkConnection) {

        try {
            JobInfo job = new JobInfo();
            job.setObject("Contact");

            job.setOperation(OperationEnum.query);
            job.setConcurrencyMode(ConcurrencyMode.Parallel);
            job.setContentType(ContentType.XML);

            job = bulkConnection.createJob(job);
            assert job.getId() != null;

            job = bulkConnection.getJobStatus(job.getId());

            String query =
                    "SELECT FirstName, LastName FROM Contact";

            long start = System.currentTimeMillis();

            BatchInfo info = null;
            ByteArrayInputStream bout =
                    new ByteArrayInputStream(query.getBytes());
            info = bulkConnection.createBatchFromStream(job, bout);

            String[] queryResults = null;
            List<String> data = new ArrayList<>();
            for (int i = 0; i < 10000; i++) {
                Thread.sleep(30000); //30 sec
                info = bulkConnection.getBatchInfo(job.getId(),
                        info.getId());

                if (info.getState() == BatchStateEnum.Completed) {
                    QueryResultList list =
                            bulkConnection.getQueryResultList(job.getId(),
                                    info.getId());
                    queryResults = list.getResult();

                    System.out.println(queryResults);
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
                for (String resultId : queryResults) {
                    InputStream inputStream = bulkConnection.getQueryResultStream(job.getId(),
                            info.getId(), resultId);

                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        JSONObject xmlJSONObj = XML.toJSONObject(line);
                        System.out.println(xmlJSONObj);
                        data.add(line);
                    }
                }
            }
            System.out.println("data size in batch --> " + data.size());
        } catch (AsyncApiException | InterruptedException aae) {
            aae.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}