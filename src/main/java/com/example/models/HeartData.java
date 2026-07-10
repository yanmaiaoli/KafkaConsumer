package com.example.models;

import com.google.gson.annotations.SerializedName;

public class HeartData {
    private String sessionId;
    private String observer;
    @SerializedName("time")
    private String timestamp;

    private int bpm;

    public HeartData() {}

    public HeartData(String sessionId, String observer, String timestamp, int bpm) {
        this.sessionId = sessionId;
        this.observer = observer;
        this.timestamp = timestamp;
        this.bpm = bpm;
    }

    public String getSessionId() { return sessionId; }
    public String getObserver() { return observer; }
    public String getTimestamp() { return timestamp; }
    public int getBpm() { return bpm; }

    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    public void setObserver(String observer) { this.observer = observer; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setBpm(int bpm) { this.bpm = bpm; }
}