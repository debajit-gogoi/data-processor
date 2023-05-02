package com.debajit.java.dataProcessor.utils;

import com.debajit.java.dataProcessor.structs.ParticipantDetails;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author debajit
 */
public class ZoomGetParticipantsUtils {

    static Logger logger = Logger.getLogger(ZoomGetParticipantsUtils.class);

    /**
     * get list of zoom participants
     *
     * @param uri       zoom uri
     * @param meetingId meeting id
     * @param type      type of fetch
     * @param pageSize  page size
     * @param token     jwt token
     * @return
     */
    public static List<String> getZoomParticipants(String uri, String meetingId, String type, int pageSize, String token) {
        List<String> participantDetailsList = new ArrayList<>();
        String nextPageToken = null;
        do {
            String zoomUrl;
            zoomUrl = uri + "/metrics/meetings/" + meetingId + "/participants?type=" + type + "&page_size=" + pageSize;
            if (nextPageToken != null) {
                zoomUrl = zoomUrl + "&next_page_token=" + nextPageToken;
            }
            try {
                nextPageToken = callApi(zoomUrl, token, participantDetailsList, meetingId, nextPageToken);
            } catch (Exception e) {
                e.printStackTrace();
                logger.warn("Zoom Api call " + zoomUrl + " to fetch participant data failed!!");
            }

            if (nextPageToken != null && nextPageToken.length() == 0) {
                nextPageToken = null;
            }
        } while (nextPageToken != null);
        return participantDetailsList;
    }

    /**
     * fetch json key from json object else ""
     *
     * @param jsonObject json object
     * @param fieldName  field name to check
     * @return
     */
    public static String validateAndFetch(JSONObject jsonObject, String fieldName) {
        if (jsonObject.has(fieldName)) {
            return jsonObject.getString(fieldName);
        } else
            return "";
    }

    /**
     * create pojo of participantDetails
     *
     * @param jsonObject json object
     * @param meetingId  meeting id
     * @return
     */
    public static ParticipantDetails getParticipantDetails(JSONObject jsonObject, String meetingId) {
        String joinTime = validateAndFetch(jsonObject, "join_time");
        String ipAddress = validateAndFetch(jsonObject, "ip_address");
        String device = validateAndFetch(jsonObject, "device");
        String id = validateAndFetch(jsonObject, "id");
        String participantUserId = validateAndFetch(jsonObject, "participant_user_id");
        String userId = validateAndFetch(jsonObject, "user_id");
        String userName = validateAndFetch(jsonObject, "user_name");
        String audioQuality = validateAndFetch(jsonObject, "audio_quality");
        String videoQuality = validateAndFetch(jsonObject, "video_quality");
        String location = validateAndFetch(jsonObject, "location");
        return new ParticipantDetails(joinTime, ipAddress, device, id, participantUserId, userId, userName, audioQuality, videoQuality, location, meetingId);
    }

    /**
     * call the list participants api
     *
     * @param zoomUrl                zoom url
     * @param token                  jwt token
     * @param participantDetailsList list of participantsdetails to add
     * @param meetingId              meeting id
     * @param nextPageToken          next page token
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static String callApi(String zoomUrl, String token, List<String> participantDetailsList, String meetingId, String nextPageToken) throws IOException, InterruptedException {
        URL url = new URL(zoomUrl);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestProperty("Accept", "application/json");
        http.setRequestProperty("Authorization", "Bearer " + token);
        int responseCode = http.getResponseCode();
        if (responseCode == 429) {

            /**
             * sleep for the rate limit
             */
            Thread.sleep(500);
            http = (HttpURLConnection) url.openConnection();
            http.setRequestProperty("Accept", "application/json");
            http.setRequestProperty("Authorization", "Bearer " + token);
        }
        BufferedReader in = new BufferedReader(
                new InputStreamReader(http.getInputStream()));
        String inputLine;
        inputLine = in.readLine();

        JSONObject json = new JSONObject(inputLine);
        JSONArray jsonArray = json.getJSONArray("participants");
        int size = jsonArray.length();
        for (int i = 0; i < size; i++) {
            participantDetailsList
                    .add(ParticipantDetails.toJson(getParticipantDetails(jsonArray.getJSONObject(i), meetingId)));
        }
        nextPageToken = (String) json.get("next_page_token");
        return nextPageToken;
    }
}
