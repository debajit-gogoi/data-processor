package com.debajit.java.dataProcessor.structs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author debajit
 */
public class ParticipantDetails {
    String joinTime;
    String ipAddress;
    String device;
    String id;
    String participantUserId;
    String userId;
    String userName;
    String audioQuality;
    String videoQuality;
    String meetingId;

    String location;

    public static final ObjectMapper mapper = new ObjectMapper();

    public ParticipantDetails() {

    }

    public ParticipantDetails(String joinTime, String ipAddress, String device, String id, String participantUserId, String userId, String userName, String audioQuality, String videoQuality, String location, String meetingId) {
        this.joinTime = joinTime;
        this.ipAddress = ipAddress;
        this.device = device;
        this.id = id;
        this.participantUserId = participantUserId;
        this.userId = userId;
        this.userName = userName;
        this.audioQuality = audioQuality;
        this.videoQuality = videoQuality;
        this.location = location;
        this.meetingId = meetingId;
    }

    public String getJoinTime() {
        return joinTime;
    }

    public void setJoinTime(String joinTime) {
        this.joinTime = joinTime;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParticipantUserId() {
        return participantUserId;
    }

    public void setParticipantUserId(String participantUserId) {
        this.participantUserId = participantUserId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getAudioQuality() {
        return audioQuality;
    }

    public void setAudioQuality(String audioQuality) {
        this.audioQuality = audioQuality;
    }

    public String getVideoQuality() {
        return videoQuality;
    }

    public void setVideoQuality(String videoQuality) {
        this.videoQuality = videoQuality;
    }

    public String getMeetingId() {
        return meetingId;
    }

    public void setMeetingId(String meetingId) {
        this.meetingId = meetingId;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public static String toJson(ParticipantDetails participantDetails) throws JsonProcessingException {
        return mapper.writeValueAsString(participantDetails);
    }
}
