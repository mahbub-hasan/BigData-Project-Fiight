import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

public class Fly implements Serializable {
    //"airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price int").csv("DataSet_Train.csv");
    private String airline;
    private String month;
    private String day_of_the_week;
    private String source;
    private Boolean source_busy;
    private String destination;
    private Boolean destination_busy;
    private ArrayList<String> route;
    private String dep_timeZone;
    private int duration;
    private int total_stops;
    private int price;


    public Fly(String airline, String month, String day_of_the_week, String source, Boolean source_busy, String destination, Boolean destination_busy, ArrayList<String> route, String dep_timeZone, int duration, int total_stops, int price) {
        this.airline = airline;
        this.month = month;
        this.day_of_the_week = day_of_the_week;
        this.source = source;
        this.source_busy = source_busy;
        this.destination = destination;
        this.destination_busy = destination_busy;
        this.route = route;
        this.dep_timeZone = dep_timeZone;
        this.duration = duration;
        this.total_stops = total_stops;
        this.price = price;
    }

    public Fly(){}

    public String getAirline() {
        return airline;
    }

    public String getMonth() {
        return month;
    }

    public String getDay_of_the_week() {
        return day_of_the_week;
    }

    public String getSource() {
        return source;
    }

    public Boolean getSource_busy() {
        return source_busy;
    }

    public String getDestination() {
        return destination;
    }

    public Boolean getDestination_busy() {
        return destination_busy;
    }

    public ArrayList<String> getRoute() {
        return route;
    }

    public String getDep_timeZone() {
        return dep_timeZone;
    }

    public int getDuration() {
        return duration;
    }

    public int getTotal_stops() {
        return total_stops;
    }

    public int getPrice() {
        return price;
    }


    public void setAirline(String airline) {
        this.airline = airline;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public void setDay_of_the_week(String day_of_the_week) {
        this.day_of_the_week = day_of_the_week;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setSource_busy(Boolean source_busy) {
        this.source_busy = source_busy;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setDestination_busy(Boolean destination_busy) {
        this.destination_busy = destination_busy;
    }

    public void setRoute(ArrayList<String> route) {
        this.route = route;
    }

    public void setDep_timeZone(String dep_timeZone) {
        this.dep_timeZone = dep_timeZone;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public void setTotal_stops(int total_stops) {
        this.total_stops = total_stops;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fly fly = (Fly) o;
        return duration == fly.duration && total_stops == fly.total_stops && price == fly.price && airline.equals(fly.airline) && month.equals(fly.month) && day_of_the_week.equals(fly.day_of_the_week) && source.equals(fly.source) && source_busy.equals(fly.source_busy) && destination.equals(fly.destination) && destination_busy.equals(fly.destination_busy) && route.equals(fly.route) && dep_timeZone.equals(fly.dep_timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(airline, month, day_of_the_week, source, source_busy, destination, destination_busy, route, dep_timeZone, duration, total_stops, price);
    }
}
