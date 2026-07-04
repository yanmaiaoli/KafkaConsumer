package com.example.models;

public class LeqData {
    private String sessionId;
    private String timestamp;
    private String location;
    private String classroom;
    private double leq;
    
    public LeqData() {}
    
    public LeqData(String sessionId, String timestamp, String location, String classroom, double leq) {
        this.sessionId = sessionId;
        this.timestamp = timestamp;
        this.location = location;
        this.classroom = classroom;
        this.leq = leq;
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
    
    public double getLeq() { return leq; }
    public void setLeq(double leq) { this.leq = leq; }
}