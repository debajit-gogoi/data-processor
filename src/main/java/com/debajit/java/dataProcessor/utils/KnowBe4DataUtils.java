package com.debajit.java.dataProcessor.utils;

import org.javatuples.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author debajit
 */
public class KnowBe4DataUtils {

    /**
     * get knowbe4 data
     *
     * @param uri      uri
     * @param token    token
     * @param pageSize page size
     * @param page     page number
     * @return
     */
    public static List<JSONObject> getKnowBe4Data(String uri, String token, int pageSize, int page) throws Exception {
        List<JSONObject> jsonArrays = new ArrayList<>();
        String knowBe4Url;
        knowBe4Url = uri + "?page=" + page + "&per_page=" + pageSize;
        try{
            Pair<BufferedReader, HttpURLConnection> pair = HTTPUtils.getHTTPGetReader(knowBe4Url, "application/json", Optional.ofNullable(token));
            HttpURLConnection http = pair.getValue1();
            BufferedReader in = pair.getValue0();
            String inputLine;
            inputLine = in.readLine();
            JSONArray json = new JSONArray(inputLine);
            for (int i = 0; i < json.length(); i++) {
                jsonArrays.add(json.getJSONObject(i));
            }
            in.close();
            http.disconnect();
            return jsonArrays;
        }
        catch (Exception ex){
            ex.printStackTrace();
            throw ex;
        }
    }
}