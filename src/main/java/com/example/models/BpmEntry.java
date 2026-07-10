package com.example.models;

public class BpmEntry {
    private String timestamp;
    private int bpm;
        
    public BpmEntry(String timestamp, int bpm) {
        this.timestamp = timestamp;
        this.bpm = bpm;
    }
        
    public String getTimestamp() { return timestamp; }
    public int getBpm() { return bpm; }
}