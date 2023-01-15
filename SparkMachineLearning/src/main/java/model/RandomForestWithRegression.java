package model;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RandomForestWithRegression {

    public RandomForestWithRegression() {
    }

    public Dataset<Row> applyModel(Dataset<Row> trainingSet, Dataset<Row> testSet){

        StringIndexer textToInt = new StringIndexer().setInputCols(new String[]{
                        "airline","month", "day_of_the_week", "arrival_timeZone"})
                .setOutputCols(new String[]{
                        "airlineIndex","monthIndex","day_of_the_weekIndex","arrival_timeZoneIndex"}).setHandleInvalid("skip");

        OneHotEncoder encoder=new OneHotEncoder()
                .setInputCols(new String[]{"airlineIndex","monthIndex","day_of_the_weekIndex","arrival_timeZoneIndex"})
                .setOutputCols(new String[]{"airlineIndexEnc","monthIndexEnc","day_of_the_weekIndexEnc","arrival_timeZoneIndexEnc"});

        VectorAssembler assembler=new VectorAssembler()
                .setInputCols(new String[]{"airlineIndexEnc","monthIndexEnc","day_of_the_weekIndexEnc","destination_busy","arrival_timeZoneIndexEnc", "busy_Intermediate","total_stops"})
                .setOutputCol("features");

        RandomForestRegressor rf=new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features");

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{textToInt,encoder,assembler,rf});

        ParamMap[] paramGrid = new ParamGridBuilder().addGrid(rf.maxDepth(), new int[] {5,10, 15}).build();

        CrossValidator cv=new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator()).setEstimatorParamMaps(paramGrid).setNumFolds(3);

        CrossValidatorModel crossValidatorModel=cv.fit(trainingSet);

        return crossValidatorModel.transform(testSet);
    }
}
