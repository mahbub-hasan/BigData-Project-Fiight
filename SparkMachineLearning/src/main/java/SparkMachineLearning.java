import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import scala.Array;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import utilities.Commons;

import java.util.List;

import static org.apache.spark.sql.functions.collect_list;

public class SparkMachineLearning {
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

        //List<Row> rows = datasetResultPoint2MapReduce.collectAsList();
        //Dataset<Row> preProcessingDataSetTrain = Preparation.preProcessing(datasetResultPoint2MapReduce, trainDataset);


        //List<Row> p=broadcast.getValue();
        Preparation preparation=new Preparation(spark,datasetResultPoint2MapReduce);
        Dataset<Fly> trainData =Preparation.transform(trainDataset,dailyAverageOfAllAirport);
        trainData.show(100);

        //preparation.transform(trainDataset, dailyAverageOfAllAirport);
/*
        Dataset<Fly> trainData = Preparation.transform(trainDataset, dailyAverageOfAllAirport);

        trainData.show(100);*/

        //Dataset<Row> trainDF = trainData.select( "month", "day_of_the_week", "destination_busy", "arrival_timeZone", "busy_Intermediate", "price");

        // store trainData into local disk
        //trainDF.write().option("header","true").format("csv").save("train");
//
        //Dataset<Row> dataTrain = trainDF.withColumnRenamed("price", "label");
//
        //Dataset<Row>[] datasets = dataTrain.randomSplit(new double[]{0.6,0.4},11L);
        //Dataset<Row> train=datasets[0];
        //Dataset<Row> test=datasets[1];
//
        //double resultOfDT = new DecisionTreeWithRegression().applyModel(train, test);
        //double resultOfRF = new RandomForestWithRegression().applyModel(train,test);

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