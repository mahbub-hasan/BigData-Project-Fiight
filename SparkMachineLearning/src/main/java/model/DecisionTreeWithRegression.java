package model;

import data_preprocessing.Fly;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil;

public class DecisionTreeWithRegression {

    public DecisionTreeWithRegression() {

    }

    /**
     * Month
     * Day_of_the_Week
     * Destination_Busy
     * arrival_timeZone
     * busy_Intermediate
     * price
     */

    public double applyModel(Dataset<Row> trainingSet, Dataset<Row> testSet, String... data){

        StringIndexer textToInt=new StringIndexer().setInputCols(new String[]{
                "month", "day_of_the_week", "arrival_timeZone"})
                .setOutputCols(new String[]{
                        "monthIndex","day_of_the_weekIndex","arrival_timeZoneIndex"});

        OneHotEncoder encoder=new OneHotEncoder()
                .setInputCols(new String[]{"monthIndex","day_of_the_weekIndex","arrival_timeZoneIndex"})
                .setOutputCols(new String[]{"monthIndexEnc","day_of_the_weekIndexEnc","arrival_timeZoneIndexEnc"});

        VectorAssembler assembler = null;
        if(data.length==0){
            assembler=new VectorAssembler()
                    .setInputCols(new String[]{"monthIndexEnc","day_of_the_weekIndexEnc","destination_busy","arrival_timeZoneIndexEnc", "busy_Intermediate"})
                    .setOutputCol("features");
        }else if(data.length==1){
            assembler=new VectorAssembler()
                    .setInputCols(new String[]{"monthIndexEnc","day_of_the_weekIndexEnc","destination_busy","arrival_timeZoneIndexEnc", "busy_Intermediate", "total_stops"})
                    .setOutputCol("features");
        }else{
            assembler=new VectorAssembler()
                    .setInputCols(new String[]{"monthIndexEnc","day_of_the_weekIndexEnc","destination_busy","arrival_timeZoneIndexEnc", "busy_Intermediate", "total_stops","duration"})
                    .setOutputCol("features");
        }

        DecisionTreeRegressor dt=new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features");

        Pipeline pipeline=new Pipeline().setStages(new PipelineStage[]{textToInt,encoder,assembler,dt});

        ParamMap[] paramGrid = new ParamGridBuilder().addGrid(dt.maxDepth(), new int[] {5,10,15}).build();

        CrossValidator cv=new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator())
                .setEstimatorParamMaps(paramGrid).setNumFolds(3);

        CrossValidatorModel crossValidatorModel=cv.fit(trainingSet);

        Dataset<Row> predictions=crossValidatorModel.transform(testSet);

        //predictions.show(5);

        RegressionEvaluator evaluator=new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse");

        return evaluator.evaluate(predictions);
    }
}
