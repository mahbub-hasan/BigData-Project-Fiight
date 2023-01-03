import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utilities.Commons;

public class SparkMachineLearning {
    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder().appName("SparkMachineLearning").master("local").getOrCreate();
        Preparation preparation=new Preparation(spark);
        Dataset<Row> trainDataset=spark.read().option("delimiter",";").option("header","true")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price double")
                .csv(Commons.TRAIN_DATASET);
        Dataset<Fly> trainData=preparation.transform(trainDataset);
        Dataset<Row> testDataset=spark.read().option("delimiter",";").option("header","true")
                .schema("airline string, date string, source string, destination string, route string, dep_time string, arrival_time string, duration string, total_stops string, additional_info string, price double")
                .csv(Commons.TEST_DATASET);
        Dataset<Fly> testData=preparation.transform(testDataset);

        trainData.show();
        testData.show();
    }


}