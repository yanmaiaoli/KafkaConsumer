// models/HighAndLowsData.java
package com.example.models;

public class HighAndLowsData {
    private String sessionId;
    private String timestamp;
    private String location;
    private String classroom;
    private double l10;
    private double l90;
    private double intrusion; // l10 - l90
    
    public HighAndLowsData() {}
    
    public HighAndLowsData(String sessionId, String timestamp, String location, String classroom, 
                          double l10, double l90) {
        this.sessionId = sessionId;
        this.timestamp = timestamp;
        this.location = location;
        this.classroom = classroom;
        this.l10 = l10;
        this.l90 = l90;
        this.intrusion = l10 - l90;
    }
    
    // Getters e Setters
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    
    public String getClassroom() { return classroom; }
    public void setClassroom(String classroom) { this.classroom = classroom; }
    
    public double getL10() { return l10; }
    public void setL10(double l10) { 
        this.l10 = l10;
        this.intrusion = this.l10 - this.l90;
    }
    
    public double getL90() { return l90; }
    public void setL90(double l90) { 
        this.l90 = l90;
        this.intrusion = this.l10 - this.l90;
    }
    
    public double getIntrusion() { return intrusion; }
}