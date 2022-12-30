import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.Commons;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

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

        datasetResultPoint3.show();
        datasetResultPoint2.show();
        originalDataset.show();


        Calendar c=Calendar.getInstance();

        Dataset<Fly> flys=originalDataset.map(new MapFunction<Row, Fly>() {
            @Override
            public Fly call(Row value) throws Exception {
                String airline = value.getAs("airline");
                String data=value.getAs("date");
                String day_of_the_week=getDayOfTheWeek(data);
                String month=getMonth(data);
                return new Fly(airline,month,day_of_the_week,null,null,null,null,null,null,0,0,0);
            }
        },Encoders.bean(Fly.class));

        flys.show();
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
}
