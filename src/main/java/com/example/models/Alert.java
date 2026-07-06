package com.example.models;

public class Alert {
    private AlertType type;
    private String sessionId;
    private String timestamp;
    private String location;
    private String classroom;
    private Severity severity;
    private String message;
    
    public enum AlertType {
        CRITICAL_INTELLIGIBILITY,
        HIGH_PEAK,
        HIGH_BACKGROUND_NOISE,
        HIGH_INTRUSION
    }
    
    public enum Severity {
        LOW, MEDIUM, HIGH, CRITICAL
    }
    
    // Construtores
    public Alert() {}
    
    public Alert(AlertType type, String sessionId, String timestamp, String location, 
                 String classroom, Severity severity, String message) {
        this.type = type;
        this.sessionId = sessionId;
        this.timestamp = timestamp;
        this.location = location;
        this.classroom = classroom;
        this.severity = severity;
        this.message = message;
    }
    
    public AlertType getType() { return type; }
    public void setType(AlertType type) { this.type = type; }
    
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }
    
    public String getClassroom() { return classroom; }
    public void setClassroom(String classroom) { this.classroom = classroom; }
    
    public Severity getSeverity() { return severity; }
    public void setSeverity(Severity severity) { this.severity = severity; }
    
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}