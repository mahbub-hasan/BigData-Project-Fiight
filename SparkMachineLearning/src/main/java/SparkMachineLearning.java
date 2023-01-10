import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import model.DecisionTreeWithRegression;
import model.RandomForestWithRegression;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
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

        Dataset<Row> trainDataset=spark.read().option("delimiter",";").option("header","true")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price double")
                .csv(Commons.TRAIN_DATASET);

        double dailyAverageOfAllAirport = datasetResultPoint3MapReduce
                .select(datasetResultPoint3MapReduce.col("real_average"))
                .where(datasetResultPoint3MapReduce.col("name_avg").equalTo("Avg"))
                .first().getDouble(0);

        Dataset<Row> preProcessingDataSetTrain = Preparation.preProcessing(datasetResultPoint2MapReduce, trainDataset);

        Dataset<Fly> trainData = Preparation.transform(preProcessingDataSetTrain, dailyAverageOfAllAirport);

        Dataset<Row> trainDF = trainData.select("airline", "month", "day_of_the_week", "source", "source_busy",
                "destination", "destination_busy", "dep_timeZone", "arrival_timeZone", "duration", "total_stops",
                "busy_Intermediate", "price");

        Dataset<Row> dataTrain = trainDF.withColumnRenamed("price", "label");

        Dataset<Row>[] datasets = dataTrain.randomSplit(new double[]{0.8,0.2},42);
        Dataset<Row> train=datasets[0];
        Dataset<Row> test=datasets[1];

        new DecisionTreeWithRegression(train, test);
        new RandomForestWithRegression(train,test);

    }


}