package data_preprocessing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Fly{
    //"airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price int").csv("DataSet_Train.csv");
    private String airline;
    private String month;
    private String day_of_the_week;
    private String source;
    private int source_busy;
    private String destination;
    private int destination_busy;
    private List<String> route;
    private String dep_timeZone;
    private String arrival_timeZone;
    private int duration;
    private int total_stops;
    private double price;
    private int busy_Intermediate;


    public Fly(String airline, String month, String day_of_the_week, String source, int source_busy, String destination, int destination_busy, List<String> route, String dep_timeZone, String arrival_timeZone, int duration, int total_stops, double price, int busy_Intermediate) {
        this.airline = airline;
        this.month = month;
        this.day_of_the_week = day_of_the_week;
        this.source = source;
        this.source_busy = source_busy;
        this.destination = destination;
        this.destination_busy = destination_busy;
        this.route = route;
        this.dep_timeZone = dep_timeZone;
        this.arrival_timeZone = arrival_timeZone;
        this.duration = duration;
        this.total_stops = total_stops;
        this.price = price;
        this.busy_Intermediate = busy_Intermediate;
    }

    public Fly(){}

    public String getAirline() {
        return airline;
    }

    public void setAirline(String airline) {
        this.airline = airline;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay_of_the_week() {
        return day_of_the_week;
    }

    public void setDay_of_the_week(String day_of_the_week) {
        this.day_of_the_week = day_of_the_week;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getSource_busy() {
        return source_busy;
    }

    public void setSource_busy(int source_busy) {
        this.source_busy = source_busy;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getDestination_busy() {
        return destination_busy;
    }

    public void setDestination_busy(int destination_busy) {
        this.destination_busy = destination_busy;
    }

    public List<String> getRoute() {
        return route;
    }

    public void setRoute(List<String> route) {
        this.route = route;
    }

    public String getDep_timeZone() {
        return dep_timeZone;
    }

    public void setDep_timeZone(String dep_timeZone) {
        this.dep_timeZone = dep_timeZone;
    }

    public String getArrival_timeZone() {
        return arrival_timeZone;
    }

    public void setArrival_timeZone(String arrival_timeZone) {
        this.arrival_timeZone = arrival_timeZone;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getTotal_stops() {
        return total_stops;
    }

    public void setTotal_stops(int total_stops) {
        this.total_stops = total_stops;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getBusy_Intermediate() {
        return busy_Intermediate;
    }

    public void setBusy_Intermediate(int busy_Intermediate) {
        this.busy_Intermediate = busy_Intermediate;
    }
}
