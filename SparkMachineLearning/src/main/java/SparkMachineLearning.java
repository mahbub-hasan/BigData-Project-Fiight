import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.Commons;
import java.lang.StringBuilder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import static org.apache.spark.sql.functions.*;

public class SparkMachineLearning {
    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder().appName("SparkMachineLearning").master("local").getOrCreate();

        Dataset<Row> originalDataset=spark.read().option("delimiter",";").option("header","true")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price int")
                .csv(Commons.TRAIN_DATASET);

        Dataset<Row> datasetResultPoint2=spark.read().option("delimiter","\t").option("header","false")
                .schema("airport string,average double")
                .csv(Commons.MAP_REDUCE_2);

        Dataset<Row> datasetResultPoint3=spark.read().option("delimiter","\t").option("header","false")
                .schema("name_avg string,real_average double")
                .csv(Commons.MAP_REDUCE_3);

        //datasetResultPoint3.show();
        //datasetResultPoint2.show();
        //originalDataset.show();

        datasetResultPoint3.show();

        Calendar c=Calendar.getInstance();

        Dataset<Row> val=datasetResultPoint3.select(datasetResultPoint3.col("real_average")).where(datasetResultPoint3.col("name_avg").equalTo("Avg"));
        double dailyAverageOfAllAirport=val.first().getDouble(0);

        System.out.println(originalDataset.count());
        Dataset<Row> prova=originalDataset.withColumn("index",monotonically_increasing_id());
        Dataset<Row> appoggio=prova;


        prova=prova.withColumn("newroute",explode(split(originalDataset.col("route"), " → ")));
        prova.drop("route");


        prova.show();
        /*
        Dataset<Row> onlyRouteSpitted=prova.groupBy(prova.col("index")).agg(collect_list("newroute"));
        onlyRouteSpitted.show();
        Dataset<Row> nuovo=prova.join(datasetResultPoint2, prova.col("newroute").equalTo(datasetResultPoint2.col("airport")),"inner").groupBy(prova.col("index")).agg(collect_list("average"));
        nuovo.show(false);
        Dataset<Row> ultimo= nuovo.join(appoggio,nuovo.col("index").equalTo(appoggio.col("index")),"inner").drop(nuovo.col("index"));
        ultimo= ultimo.join(onlyRouteSpitted,ultimo.col("index").equalTo(onlyRouteSpitted.col("index")),"inner");

        ultimo.show();

        Dataset<Fly> flys=ultimo.map(new MapFunction<Row, Fly>() {
            @Override
            public Fly call(Row value) throws Exception {
                String airline = value.getAs("airline");
                String source = value.getAs("source");
                String data=value.getAs("date");
                String day_of_the_week=getDayOfTheWeek(data);
                String month=getMonth(data);
                String destination = value.getAs("destination");
                String duration=durationInMinute(value.getAs("duration"));
                String total_stops=value.getAs("total_stops");
                scala.collection.Seq<Double> s=value.getAs("collect_list(average)");
                scala.collection.Seq<String> route=value.getAs("collect_list(newroute)");
                ArrayList<String> arrayRoute=new ArrayList<>();
                int price=value.getAs("price");
                String depTime=value.getAs("dep_time");
                String newDepTime=returnPeriodOfDay(depTime);
                String arrivalTime=value.getAs("arrival_time");
                String newArrivalTime=returnPeriodOfDay(arrivalTime);

                double dailyAverageOfSourceAirport=s.apply(0);
                boolean source_Busy=false;
                if(dailyAverageOfSourceAirport>dailyAverageOfAllAirport) {
                    source_Busy = true;
                }

                double dailyAverageOfDestinationAirport=s.apply(0);
                boolean destination_Busy=false;
                if(dailyAverageOfDestinationAirport>dailyAverageOfAllAirport) {
                    destination_Busy = true;
                }

                boolean busy_Intermediate=false;
                for(int i=0;i<s.length();i++){
                    if(i!=0 && i!=s.length()-1){
                        if(s.apply(i)>dailyAverageOfAllAirport){
                            busy_Intermediate=true;
                        }
                    }
                }

                for(int i=0;i<route.length();i++){
                    if(i!=0 && i!=s.length()-1) {
                        arrayRoute.add(route.apply(i));
                    }
                }

                return new Fly(airline,month,day_of_the_week,source,source_Busy,destination,destination_Busy,arrayRoute,newDepTime,newArrivalTime,duration,total_stops,price,busy_Intermediate);
            }
        },Encoders.bean(Fly.class));

        flys.show();
        */
    }

    public static String getDayOfTheWeek(String input_date){
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

    public static String getMonth(String input_date){
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

    public static String getNameOfWeekInEnglish(String day){
        if(day.equalsIgnoreCase("lunedì") || day.equalsIgnoreCase("monday")){
            return "Monday";
        }
        else if(day.equalsIgnoreCase("martedì") || day.equalsIgnoreCase("tuesday")){
            return "Tuesday";
        }
        else if(day.equalsIgnoreCase("mercoledì") || day.equalsIgnoreCase("wednesday")){
            return "Wednesday";
        }
        else if(day.equalsIgnoreCase("giovedì") || day.equalsIgnoreCase("thursday")){
            return "Thursday";
        }
        else if(day.equalsIgnoreCase("venerdì") || day.equalsIgnoreCase("friday")){
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

    public static String getNameOfMonth(String month){
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

    public static String durationInMinute(String duration){
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

    public static String returnPeriodOfDay(String time){
        String []onlyHour=time.split(":");
        int hour=Integer.parseInt(onlyHour[0]);
        return compareHoursAndPeriod(hour);
    }

    public static String compareHoursAndPeriod(int hour){
        String partOfTheDay=null;
        if(hour>=0 && hour<6){
            partOfTheDay="night";
        }
        else if(hour>=6 && hour<8){
            partOfTheDay="early morning";
        }
        else if(hour>=8 && hour<14){
            partOfTheDay="morning";
        }
        else if(hour>=14 && hour<18){
            partOfTheDay="afternoon";
        }
        else if(hour>=18){
            partOfTheDay="evening";
        }
        return partOfTheDay;
    }

}