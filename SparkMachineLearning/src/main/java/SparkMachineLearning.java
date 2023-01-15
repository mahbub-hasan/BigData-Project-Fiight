import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import model.DecisionTreeWithRegression;
import model.RandomForestWithRegression;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import utilities.Commons;
import utilities.DataTransfer;

import java.util.List;

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


        Dataset<Row> trainDF_For_DT = trainData.select( "airline","month", "day_of_the_week", "destination_busy", "arrival_timeZone", "total_stops","busy_Intermediate","price");
        Dataset<Row> trainDF_For_RF = trainData.select( "airline","month", "day_of_the_week", "destination_busy", "arrival_timeZone", "total_stops","busy_Intermediate","price");

        Dataset<Row> dataTrain_For_DT = trainDF_For_DT.withColumnRenamed("price", "label");
        Dataset<Row> dataTrain_For_RF = trainDF_For_RF.withColumnRenamed("price", "label");


        Dataset<Row>[] train_test_split_DT = dataTrain_For_DT.randomSplit(new double[]{0.6,0.4},42);
        Dataset<Row>[] train_test_split_RF = dataTrain_For_RF.randomSplit(new double[]{0.6,0.4},42);

        Dataset<Row> train_data_DT=train_test_split_DT[0];
        Dataset<Row> test_data_DT=train_test_split_DT[1];

        Dataset<Row> train_data_RF=train_test_split_RF[0];
        Dataset<Row> test_data_RF=train_test_split_RF[1];

        Dataset<Row> prediction_result_DT = new DecisionTreeWithRegression().applyModel(train_data_DT, test_data_DT);
        RegressionEvaluator evaluator_DT_rmse = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse");
        RegressionEvaluator evaluator_DT_r2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2");
        double resultOfDT_RMSE = evaluator_DT_rmse.evaluate(prediction_result_DT);
        double resultOfDT_R2 = evaluator_DT_r2.evaluate(prediction_result_DT);



        Dataset<Row> prediction_result_RF = new RandomForestWithRegression().applyModel(train_data_RF,test_data_RF);
        RegressionEvaluator evaluator_RF_rmse = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse");
        RegressionEvaluator evaluator_RF_r2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2");
        double resultOfRF_RMSE = evaluator_RF_rmse.evaluate(prediction_result_RF);
        double resultOfRF_R2 = evaluator_RF_r2.evaluate(prediction_result_RF);


        System.out.println("********************************************");
        System.out.println();
        System.out.println("RMSE DECISION TREE "+ resultOfDT_RMSE);
        System.out.println("R2 DECISION TREE "+ resultOfDT_R2);
        System.out.println();
        System.out.println("********************************************");
        System.out.println("********************************************");
        System.out.println();
        System.out.println("RMSE RANDOM FOREST "+ resultOfRF_RMSE);
        System.out.println("R2 RANDOM FOREST "+ resultOfRF_R2);
        System.out.println();
        System.out.println("********************************************");
    }
}