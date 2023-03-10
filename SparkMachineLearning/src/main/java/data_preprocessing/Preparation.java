package data_preprocessing;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import utilities.DataTransfer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Preparation implements DataTransfer {

    static Broadcast<List<Row>> broadcast;

    @Override
    public void setBroadcast(Broadcast<List<Row>> data) {
        broadcast = data;
    }

    public static Dataset<Fly> transform(Dataset<Row> initialDataset, double dailyAverageOfAllAirport){

        return initialDataset.map((MapFunction<Row, Fly>) value -> {
            // list of route
            List<String> route = getRoute(value.getAs("route"));
            // list of average
            List<Double> avg = new ArrayList<>();
            route.forEach(airport->{
                broadcast.getValue().forEach(info->{
                    if(info.getAs("airport").equals(airport)){
                        avg.add(Double.parseDouble(info.getAs("average").toString()));
                    }
                });
            });
            // airline
            String airline = value.getAs("airline");
            // Month of the journey
            String month=getMonth(value.getAs("date"));
            // DayOfWeek of the journey
            String day_of_the_week=getDayOfTheWeek(value.getAs("date"));
            // source of the journey
            String source = value.getAs("source");

            // source busy information of the journey
            boolean source_Busy=false;
            try{
                if(avg.get(0) >dailyAverageOfAllAirport) {
                    source_Busy = true;
                }
            }catch (Exception ex){
                route.forEach(System.out::println);
                System.out.println(route.size() +"-----------"+avg.size());
            }

            // destination of the journey
            String destination = value.getAs("destination");
            // destination busy information of the journey
            boolean destination_Busy=false;
            if(avg.get(avg.size()-1)>dailyAverageOfAllAirport) {
                destination_Busy = true;
            }

            // intermediate stops busy or not
            boolean busy_Intermediate=false;
            ArrayList<String> intermediateRouteList = new ArrayList<>();
            for(int i=0;i<avg.size();i++){
                if(i!=0 && i!=avg.size()-1){
                    if(avg.get(i)>dailyAverageOfAllAirport){
                        busy_Intermediate=true;
                        intermediateRouteList.add(route.get(i));
                    }
                }
            }
            // Dep TimeZone of the journey
            String deptTimeZone = returnPeriodOfDay(value.getAs("dep_time"));

            // Arrival TimeZone of the journey
            String arrivalTimeZone = returnPeriodOfDay(value.getAs("arrival_time"));

            // duration of the journey
            String duration=durationInMinute(value.getAs("duration"));

            // totalStops of the journey
            int total_stops = intermediateRouteList.size();

            // price of the journey
            double price = 0.0;
            try{
                price=value.getAs("price");
            }catch (Exception ex){
            }

            return new Fly(
                    airline,
                    month,
                    day_of_the_week,
                    source,
                    source_Busy?1:0,
                    destination,
                    destination_Busy?1:0,
                    route,
                    deptTimeZone,
                    arrivalTimeZone,
                    Integer.parseInt(duration),
                    total_stops,
                    price,
                    busy_Intermediate?1:0);
        }, Encoders.bean(Fly.class));
    }

    private static List<String> getRoute(String route){
        return Arrays.asList(route.split(" ??? "));
    }

//    private List<Double> getAvg(List<String> airports, Broadcast<List<Row>> broadcast){
//        List<Double> value = new ArrayList<>();
//        airports.forEach(airport->{
//            broadcast.getValue().forEach(info->{
//                if(info.getAs("airport").equals(airport)){
//                    value.add(Double.parseDouble(info.getAs("average").toString()));
//                }
//            });
//        });
//
//        return value;
//    }

    private static String getDayOfTheWeek(String input_date){
        try{
            SimpleDateFormat format1 = new SimpleDateFormat("dd/MM/yyyy");
            Date dt1 = format1.parse(input_date);
            DateFormat format2 = new SimpleDateFormat("EEEE");
            String finalDay = format2.format(dt1);
            return getNameOfWeekInEnglish(finalDay);
        }catch (Exception e){
            System.err.println("Error");
            return null;
        }
    }

    private static String getMonth(String input_date){
        try{
            SimpleDateFormat format1 = new SimpleDateFormat("dd/MM/yyyy");
            Date dt1 = format1.parse(input_date);
            DateFormat format3 = new SimpleDateFormat("MMMM");
            String finalMonth = format3.format(dt1);
            return getNameOfMonth(finalMonth);
        }catch (Exception e){
            System.err.println("Error");
            return null;
        }
    }

    private static String getNameOfWeekInEnglish(String day){
        if(day.equalsIgnoreCase("luned??") || day.equalsIgnoreCase("monday")){
            return "Monday";
        }
        else if(day.equalsIgnoreCase("marted??") || day.equalsIgnoreCase("tuesday")){
            return "Tuesday";
        }
        else if(day.equalsIgnoreCase("mercoled??") || day.equalsIgnoreCase("wednesday")){
            return "Wednesday";
        }
        else if(day.equalsIgnoreCase("gioved??") || day.equalsIgnoreCase("thursday")){
            return "Thursday";
        }
        else if(day.equalsIgnoreCase("venerd??") || day.equalsIgnoreCase("friday")){
            return "Friday";
        }
        else if(day.equalsIgnoreCase("sabato") || day.equalsIgnoreCase("saturday")){
            return "Saturday";
        }
        else if(day.equalsIgnoreCase("domenica") || day.equalsIgnoreCase("sunday")){
            return "Sunday";
        }
        else{
            return null;
        }
    }

    private static String getNameOfMonth(String month){
        if(month.equalsIgnoreCase("gennaio") || month.equalsIgnoreCase("january")){
            return "January";
        }
        else if(month.equalsIgnoreCase("febbraio") || month.equalsIgnoreCase("february")){
            return "February";
        }
        else if(month.equalsIgnoreCase("marzo") || month.equalsIgnoreCase("march")){
            return "March";
        }
        else if(month.equalsIgnoreCase("aprile") || month.equalsIgnoreCase("april")){
            return "April";
        }
        else if(month.equalsIgnoreCase("maggio") || month.equalsIgnoreCase("may")){
            return "May";
        }
        else if(month.equalsIgnoreCase("giugno") || month.equalsIgnoreCase("june")){
            return "June";
        }
        else if(month.equalsIgnoreCase("luglio") || month.equalsIgnoreCase("july")){
            return "July";
        }
        else if(month.equalsIgnoreCase("agosto") || month.equalsIgnoreCase("august")){
            return "August";
        }
        else if(month.equalsIgnoreCase("settembre") || month.equalsIgnoreCase("september")){
            return "September";
        }
        else if(month.equalsIgnoreCase("ottobre") || month.equalsIgnoreCase("october")){
            return "October";
        }
        else if(month.equalsIgnoreCase("novembre") || month.equalsIgnoreCase("november")){
            return "November";
        }
        else if(month.equalsIgnoreCase("dicembre") || month.equalsIgnoreCase("december")){
            return "December";
        }
        else{
            return null;
        }
    }

    private static String durationInMinute(String duration){
        String durationSplit[]=duration.split(" ");
        int somma=0;
        for(int i=0;i<durationSplit.length;i++){
            String nuovaStringa=durationSplit[i].substring(0,durationSplit[i].length()-1);
            String type=durationSplit[i].substring(durationSplit[i].length()-1);
            if(type.equalsIgnoreCase("h")){
                int oreToMinute=Integer.parseInt(nuovaStringa)*60;
                somma+=oreToMinute;
            }
            else if(type.equalsIgnoreCase("m")){
                int minute=Integer.parseInt(nuovaStringa);
                somma+=minute;
            }
        }
        StringBuilder result=new StringBuilder();
        result.append(somma);
        return result.toString();
    }

    //22:20
    private static String returnPeriodOfDay(String time){
        String []onlyHour=time.split(":"); // 0->22 1->20
        int hour=Integer.parseInt(onlyHour[0]);
        return compareHoursAndPeriod(hour);
    }

    /*
     * early morning -> [6 to <8]
     * morning -> [8 to <12]
     * afternoon -> [12 to <17]
     * evening -> [17 to 21]
     * night [21 to <6]
     */
    private static String compareHoursAndPeriod(int hour){
        String partOfTheDay=null;

        if(hour>=0 && hour<6){
            partOfTheDay = "night";
        }
        else if(hour>=6 && hour<8){
            partOfTheDay="early morning";
        }
        else if(hour>=8 && hour<12){
            partOfTheDay="morning";
        }
        else if(hour>=12 && hour<17){
            partOfTheDay="afternoon";
        }
        else if(hour>=17 && hour<21){
            partOfTheDay="evening";
        }else if(hour>=21){
            partOfTheDay="night";
        }
        return partOfTheDay;
    }

    public Dataset<Row> getClusterInstance(int cluster_id,String clusterColumn,Dataset<Row> data){
        return data.filter((FilterFunction<Row>) r ->{
            int predictedCluster = r.getAs(clusterColumn);
            return predictedCluster==cluster_id;
        });
    }
}
