package data_preprocessing;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.Commons;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.collect_list;

public class Preparation {

    private double dailyAverageOfAllAirport;
    private Dataset<Row> datasetResultPoint2MapReduce;
    private Dataset<Row> datasetResultPoint3MapReduce;
    private SparkSession spark;

    public Preparation(SparkSession spark){
        this.spark=spark;
        readDatasetResultPoint2Mapreduce();
        readDatasetResultPoint3Mapreduce();
        setAverageOfAllAirports();

    }

    private void readDatasetResultPoint3Mapreduce() {
        this.datasetResultPoint3MapReduce=spark.read().option("delimiter","\t").option("header","false")
                .schema("name_avg string,real_average double")
                .csv(Commons.MAP_REDUCE_3);
    }

    private void readDatasetResultPoint2Mapreduce() {
        this.datasetResultPoint2MapReduce = spark.read().option("delimiter", "\t").option("header", "false")
                .schema("airport string,average double")
                .csv(Commons.MAP_REDUCE_2);
    }

    private void setAverageOfAllAirports(){
        Dataset<Row> val=datasetResultPoint3MapReduce.select(datasetResultPoint3MapReduce.col("real_average")).where(datasetResultPoint3MapReduce.col("name_avg").equalTo("Avg"));
        this.dailyAverageOfAllAirport=val.first().getDouble(0);
    }

    public Dataset<Fly> transform(Dataset<Row> originalDataset){

        Dataset<Row> originalDatasetWithIndex=originalDataset.withColumn("index",monotonically_increasing_id());
        Dataset<Row> rememberOriginalDataset=originalDatasetWithIndex;

        originalDatasetWithIndex=originalDatasetWithIndex.withColumn("newroute",explode(split(originalDataset.col("route"), " → ")));
        originalDatasetWithIndex.drop("route");

        Dataset<Row> onlyRouteSplitted=originalDatasetWithIndex.groupBy(originalDatasetWithIndex.col("index")).agg(collect_list("newroute"));

        Dataset<Row> averageOfTheRoute=originalDatasetWithIndex.join(datasetResultPoint2MapReduce, originalDatasetWithIndex.col("newroute").equalTo(datasetResultPoint2MapReduce.col("airport")),"inner").groupBy(originalDatasetWithIndex.col("index")).agg(collect_list("average"));

        Dataset<Row> finalDataset= averageOfTheRoute.join(rememberOriginalDataset,averageOfTheRoute.col("index").equalTo(rememberOriginalDataset.col("index")),"inner").drop(averageOfTheRoute.col("index"));
        finalDataset= finalDataset.join(onlyRouteSplitted,finalDataset.col("index").equalTo(onlyRouteSplitted.col("index")),"inner");

        Dataset<Fly> flys=finalDataset.map(new MapFunction<Row, Fly>() {
            @Override
            public Fly call(Row value) throws Exception {
                // airline
                String airline = value.getAs("airline");

                // Month of the journey
                String month=getMonth(value.getAs("date"));

                // DayOfWeek of the journey
                String day_of_the_week=getDayOfTheWeek(value.getAs("date"));

                // source of the journey
                String source = value.getAs("source");

                // source busy information of the journey
                scala.collection.Seq<Double> routeAvgList=value.getAs("collect_list(average)");

                boolean source_Busy=false;
                if(routeAvgList.apply(0)>dailyAverageOfAllAirport) {
                    source_Busy = true;
                }

                // destination of the journey
                String destination = value.getAs("destination");

                // destination busy information of the journey
                boolean destination_Busy=false;
                if(routeAvgList.apply(routeAvgList.length()-1)>dailyAverageOfAllAirport) {
                    destination_Busy = true;
                }

                // list of route of the journey
                scala.collection.Seq<String> routeList=value.getAs("collect_list(newroute)");
                ArrayList<String> arrayRoute=new ArrayList<>();
                for(int i=0;i<routeList.length();i++){
                    if(i!=0 && i!=routeList.length()-1) {
                        arrayRoute.add(routeList.apply(i));
                    }
                }

                // Dep TimeZone of the journey
                String deptTimeZone = returnPeriodOfDay(value.getAs("dep_time"));

                // Arrival TimeZone of the journey
                String arrivalTimeZone = returnPeriodOfDay(value.getAs("arrival_time"));

                // duration of the journey
                String duration=durationInMinute(value.getAs("duration"));

                // totalStops of the journey
                int total_stops = arrayRoute.size();

                // price of the journey
                double price=value.getAs("price");

                // intermediate stops busy or not
                boolean busy_Intermediate=false;
                for(int i=0;i<routeAvgList.length();i++){
                    if(i!=0 && i!=routeAvgList.length()-1){
                        if(routeAvgList.apply(i)>dailyAverageOfAllAirport){
                            busy_Intermediate=true;
                        }
                    }
                }

                return new Fly(
                        airline,
                        month,
                        day_of_the_week,
                        source,
                        source_Busy?1:0,
                        destination,
                        destination_Busy?1:0,
                        arrayRoute,
                        deptTimeZone,
                        arrivalTimeZone,
                        Integer.parseInt(duration),
                        total_stops,
                        price,
                        busy_Intermediate?1:0);
            }
        }, Encoders.bean(Fly.class));
        return flys;
    }

    private String getDayOfTheWeek(String input_date){
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

    private String getMonth(String input_date){
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

    private String getNameOfWeekInEnglish(String day){
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

    private String getNameOfMonth(String month){
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

    private String durationInMinute(String duration){
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
    private String returnPeriodOfDay(String time){
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
    private String compareHoursAndPeriod(int hour){
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
}
