package com.debajit.java.dataProcessor.utils;

import org.javatuples.Pair;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author debajit
 */
public class O365DataUtils {
    /**
     * get o365 api data
     *
     * @param uri          uri
     * @param tokenUri     token uri
     * @param grantType    grant type
     * @param clientId     client id
     * @param clientSecret client secret
     * @param scope        scope
     * @return
     * @throws Exception
     */
    public static List<String> o365Data(String uri, String tokenUri, String grantType, String clientId, String clientSecret, String scope) throws Exception {
        String urlParameters = "grant_type=" + grantType + "&client_id=" + clientId + "&client_secret=" + clientSecret + "&scope=" + scope + "";
        String accessToken = HTTPUtils.callHTTPPostMethod(urlParameters, tokenUri, "application/x-www-form-urlencoded", Optional.ofNullable(null)).getString("access_token");
        Pair<BufferedReader, HttpURLConnection> pair;
        try {
            pair = HTTPUtils.getHTTPGetReader(uri, "application/json", Optional.ofNullable(accessToken));
            HttpURLConnection http = pair.getValue1();
            BufferedReader input = pair.getValue0();
            String o365Data;

            List<String> o365DataList = new ArrayList<>();
            while ((o365Data = input.readLine()) != null) {
                o365DataList.add(o365Data);
            }
            input.close();
            http.disconnect();
            return o365DataList;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        }

    }
}
