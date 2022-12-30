import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class SparkMachineLearning {
    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder().appName("SparkMachineLearning").getOrCreate();
        Dataset<Row> originalDataset=spark.read().option("delimiter",";").option("header","true").schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price int").csv("DataSet_Train.csv");
        Dataset<Row> datasetResultPoint2=spark.read().option("delimiter","\t").option("header","false").schema("airport string,average double").csv("project/output/map-reduce-2/part-r-00000");
        Dataset<Row> datasetResultPoint3=spark.read().option("delimiter","\t").option("header","false").schema("name_avg string,real_average double").csv("project/output/map-reduce-3/part-r-00000");
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
                System.out.println("I'M HEREEEEEEEEEEEEEEEEE"+data);
                System.out.println("I'M HEREE"+day_of_the_week);

                return new Fly(airline,null,day_of_the_week,null,null,null,null,null,null,0,0,0);
            }
        },Encoders.bean(Fly.class));

        flys.show();


        /*
        try {
            String input_date = "30/12/2022";
            SimpleDateFormat format1 = new SimpleDateFormat("dd/MM/yyyy");
            Date dt1 = format1.parse(input_date);
            DateFormat format2 = new SimpleDateFormat("EEEE");
            DateFormat format3 = new SimpleDateFormat("MMMM");
            String finalDay = format2.format(dt1);
            System.out.println(getNameOfWeekInEnglish(finalDay));
            String finalMonth = format3.format(dt1);
            System.out.println(finalMonth);
        }catch (Exception e){
            System.err.println("Eccezzione");
        }
        */

    }

    public static String getDayOfTheWeek(String input_date){
        try{
            //Locale locale=new Locale("en","us");
            SimpleDateFormat format1 = new SimpleDateFormat("dd/MM/yyyy");

            Date dt1 = format1.parse(input_date);
            //day of the week
            DateFormat format2 = new SimpleDateFormat("EEEE");

            //mounth
            //DateFormat format3 = new SimpleDateFormat("MMMM");

            String finalDay = format2.format(dt1);
            return getNameOfWeekInEnglish(finalDay);
        }catch (Exception e){
            System.err.println("Error");
            return null;
        }
    }

    public static String getNameOfWeekInEnglish(String day){
        switch (day){
            case "lunedì":
                return "Monday";
            case "martedì":
                return "Tuesday";
            case "mercoledì":
                return "Wednesday";
            case "giovedì":
                return "Thursday";
            case "venerdì":
                return "Friday";
            case "sabato":
                return "Saturday";
            case "domenica":
                return "Sunday";
        }
        return null;
    }


}
