import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import model.KMeansWithRegression;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.Commons;

import java.util.List;

import static org.apache.spark.sql.functions.*;
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

        Dataset<Row> testDataset=spark.read().option("delimiter",";").option("header","true")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string")
                .csv(Commons.TEST_DATASET);

        double dailyAverageOfAllAirport = datasetResultPoint3MapReduce
                .select(datasetResultPoint3MapReduce.col("real_average"))
                .where(datasetResultPoint3MapReduce.col("name_avg").equalTo("Avg"))
                .first().getDouble(0);

        Dataset<Row> preProcessingDataSetTrain = Preparation.preProcessing(datasetResultPoint2MapReduce, trainDataset);
        Dataset<Row> preProcessingDataSetTest = Preparation.preProcessing(datasetResultPoint2MapReduce, testDataset);

        Dataset<Fly> trainData = Preparation.transform(preProcessingDataSetTrain, dailyAverageOfAllAirport);
        Dataset<Fly> testData = Preparation.transform(preProcessingDataSetTest, dailyAverageOfAllAirport);

        trainData.show();
        testData.show();

        KMeansWithRegression model = new KMeansWithRegression();
        model.fit(trainData);
        List<Dataset<Row>> result = model.transform(testData);

        double sum = 0d;
        for(Dataset<Row> data : result) {
            data.persist();
            sum += data.select("prediction").as(Encoders.DOUBLE()).reduce((ReduceFunction<Double>) (f1, f2)->f1+f2);
            data.unpersist();
        }

        System.out.println(sum);
    }


}