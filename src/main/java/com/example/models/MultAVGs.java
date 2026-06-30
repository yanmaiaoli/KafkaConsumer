package com.example.models;

public class MultAVGs {
    private double sum = 0;
    private int count = 0;
    private double suml10 = 0;
    private int countl10 = 0;
    private double suml90 = 0;
    private int countl90 = 0;
    private boolean hasL10 = false;  // Flag para saber se já temos valores L10
    private boolean hasL90 = false;  // Flag para saber se já temos valores L90

    public void addValue(double value) {
        sum += value;
        count++;
    }

    public double getAverage() {
        return count > 0 ? sum / count : 0;
    }

    public int getCount() {
        return count;
    }

    public void addValuel10(double value) {
        suml10 += value;
        countl10++;
        hasL10 = true;
    }

    public double getAveragel10() {
        return hasL10 && countl10 > 0 ? suml10 / countl10 : 0;
    }

    public int getCountl10() {
        return countl10;
    }

    public void addValuel90(double value) {
        suml90 += value;
        countl90++;
        hasL90 = true;
    }

    public double getAveragel90() {
        return hasL90 && countl90 > 0 ? suml90 / countl90 : 0;
    }

    public int getCountl90() {
        return countl90;
    }

    public boolean hasL10Values() {
        return hasL10 && countl10 > 0;
    }

    public boolean hasL90Values() {
        return hasL90 && countl90 > 0;
    }
}