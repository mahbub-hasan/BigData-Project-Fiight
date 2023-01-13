import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import model.DecisionTreeWithRegression;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import scala.Array;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import utilities.Commons;
import utilities.DataTransfer;

import java.util.List;

import static org.apache.spark.sql.functions.collect_list;

public class SparkMachineLearning {
    private static Broadcast<List<Row>> broadcast;
    static DataTransfer transform = new Preparation();

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("SparkMachineLearning").master("local").getOrCreate();
        Dataset<Row> datasetResultPoint2MapReduce = spark.read().option("delimiter", "\t").option("header", "false")
                .schema("airport string,average double")
                .csv(Commons.MAP_REDUCE_2);

        Dataset<Row> datasetResultPoint3MapReduce = spark.read().option("delimiter","\t").option("header","false")
                .schema("name_avg string,real_average double")
                .csv(Commons.MAP_REDUCE_3);

        Dataset<Row> trainDataset=spark.read().option("delimiter",";")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price double")
                .csv(Commons.TRAIN_DATASET);

        double dailyAverageOfAllAirport = datasetResultPoint3MapReduce
                .select(datasetResultPoint3MapReduce.col("real_average"))
                .where(datasetResultPoint3MapReduce.col("name_avg").equalTo("Avg"))
                .first().getDouble(0);


        ClassTag<List<Row>> c= ClassTag$.MODULE$.apply(List.class);
        broadcast = spark.sparkContext().broadcast(datasetResultPoint2MapReduce.collectAsList(), c);
        transform.setBroadcast(broadcast);
        Dataset<Fly> trainData =Preparation.transform(trainDataset, dailyAverageOfAllAirport);
        trainData.show(10);

        //preparation.transform(trainDataset, dailyAverageOfAllAirport);
/*
        Dataset<Fly> trainData = Preparation.transform(trainDataset, dailyAverageOfAllAirport);

        trainData.show(100);*/

        Dataset<Row> trainDF1 = trainData.select( "month", "day_of_the_week", "destination_busy", "arrival_timeZone", "busy_Intermediate", "price");
        Dataset<Row> trainDF2 = trainData.select( "month", "day_of_the_week", "destination_busy", "arrival_timeZone", "total_stops","busy_Intermediate","price");
        Dataset<Row> trainDF3 = trainData.select( "month", "day_of_the_week", "destination_busy", "arrival_timeZone", "duration","total_stops","busy_Intermediate","price");

        // store trainData into local disk
        //trainDF.write().option("header","true").format("csv").save("train");
//
        Dataset<Row> dataTrain1 = trainDF1.withColumnRenamed("price", "label");
        Dataset<Row> dataTrain2 = trainDF2.withColumnRenamed("price", "label");
        Dataset<Row> dataTrain3 = trainDF3.withColumnRenamed("price", "label");
////
        Dataset<Row>[] datasets_test1 = dataTrain1.randomSplit(new double[]{0.8,0.2},42);
        Dataset<Row>[] datasets_test2 = dataTrain2.randomSplit(new double[]{0.8,0.2},42);
        Dataset<Row>[] datasets_test3 = dataTrain3.randomSplit(new double[]{0.8,0.2},42);
//
        Dataset<Row> train_1=datasets_test1[0];
        Dataset<Row> test_1=datasets_test1[1];

        Dataset<Row> train_2=datasets_test2[0];
        Dataset<Row> test_2=datasets_test2[1];

        Dataset<Row> train_3=datasets_test3[0];
        Dataset<Row> test_3=datasets_test3[1];
////
        double resultOfDT_1 = new DecisionTreeWithRegression().applyModel(train_1, test_1);
        double resultOfDT_2 = new DecisionTreeWithRegression().applyModel(train_2, test_2,"total_stops");
        double resultOfDT_3 = new DecisionTreeWithRegression().applyModel(train_3, test_3,"duration","total_stops");
//        //double resultOfRF = new RandomForestWithRegression().applyModel(train,test);
//
//
        System.out.println("********************************************");
        System.out.println();
        System.out.println("RMSE RANDOM FOREST "+ resultOfDT_1);
        System.out.println();
        System.out.println("********************************************");
        System.out.println("RMSE RANDOM FOREST "+ resultOfDT_2);
        System.out.println();
        System.out.println("********************************************");
        System.out.println("RMSE RANDOM FOREST "+ resultOfDT_3);
        System.out.println();
        System.out.println("********************************************");



//        System.out.println("********************************************");
//        System.out.println();
//        System.out.println("RMSE RANDOM FOREST "+ resultOfRF);
//        System.out.println();
//        System.out.println("********************************************");
//        System.out.println();
//        System.out.println("RMSE DECISION TREE "+ resultOfDT);
//        System.out.println();
//        System.out.println("********************************************");

    }


}