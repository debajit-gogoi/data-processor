package com.debajit.java.dataProcessor.utils;

import org.javatuples.Pair;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author debajit
 */
public class HTTPUtils {
    public static Pair<BufferedReader,HttpURLConnection> getHTTPGetReader(String spec, String contentType, Optional<String> accessToken) throws IOException {
        URL urlForJobStatus = new URL(spec);
        HttpURLConnection conn = (HttpURLConnection) urlForJobStatus.openConnection();
        conn.setRequestProperty("Accept", contentType);
        accessToken.ifPresent(s -> conn.setRequestProperty("Authorization", "Bearer " + s));

        return new Pair<>(new BufferedReader(
                new InputStreamReader(conn.getInputStream())), conn);
    }
    public static JSONObject callHTTPGetMethod(String spec, String contentType, Optional<String> accessToken){
        JSONObject json;
        try{
            Pair<BufferedReader, HttpURLConnection> pair  = getHTTPGetReader(spec, contentType, accessToken);
            String inputLine;
            inputLine = pair.getValue0().readLine();
            json = new JSONObject(inputLine);
            pair.getValue0().close();
            pair.getValue1().disconnect();
            return json;
        }
        catch(Exception ex){
            ex.printStackTrace();
            throw new RuntimeException();
        }

    }

    public static JSONObject callHTTPPostMethod(String urlParameters, String spec, String contentType, Optional<String> accessToken){
        byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);
        int postDataLength = postData.length;
        JSONObject json;
        try{
            URL url = new URL(spec);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", contentType);
            conn.setRequestProperty("charset", "utf-8");
            conn.setRequestProperty("Content-Length", Integer.toString(postDataLength));
            conn.setUseCaches(false);
            accessToken.ifPresent(s -> conn.setRequestProperty("Authorization", "Bearer " + s));
            try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
                wr.write(postData);
            }
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String inputLine;
            inputLine = in.readLine();

            json = new JSONObject(inputLine);
            in.close();
            conn.disconnect();
            return json;
        }
        catch(Exception ex){
            ex.printStackTrace();
            throw new RuntimeException();
        }

    }
}
