package model;

import data_preprocessing.Fly;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
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

public class DecisionTreeWithRegression {

    public DecisionTreeWithRegression(Dataset<Row> trainingSet, Dataset<Row> testSet){

        //trainingSet.show(2);
        //testSet.show(2);

        StringIndexer airlineIndexer=new StringIndexer().setInputCol("airline").setOutputCol("airlineIndex").setHandleInvalid("skip");
        StringIndexer monthIndexer=new StringIndexer().setInputCol("month").setOutputCol("monthIndex");
        StringIndexer dayOfTheWeekIndexer=new StringIndexer().setInputCol("day_of_the_week").setOutputCol("day_of_the_weekIndex");
        StringIndexer sourceIndexer=new StringIndexer().setInputCol("source").setOutputCol("sourceIndex");
        StringIndexer destinationIndexer=new StringIndexer().setInputCol("destination").setOutputCol("destinationIndex");
        StringIndexer dep_timeZoneIndexer=new StringIndexer().setInputCol("dep_timeZone").setOutputCol("dep_timeZoneIndex");
        StringIndexer arrival_timeZoneIndexer=new StringIndexer().setInputCol("arrival_timeZone").setOutputCol("arrival_timeZoneIndex");

//        airlineIndexer.fit(trainingSet).transform(trainingSet).show();
//        monthIndexer.fit(trainingSet).transform(trainingSet).show();
//        dayOfTheWeekIndexer.fit(trainingSet).transform(trainingSet).show();
//        sourceIndexer.fit(trainingSet).transform(trainingSet).show();
//        destinationIndexer.fit(trainingSet).transform(trainingSet).show();
//        dep_timeZoneIndexer.fit(trainingSet).transform(trainingSet).show();
//        arrival_timeZoneIndexer.fit(trainingSet).transform(trainingSet).show();


        OneHotEncoder encoder=new OneHotEncoder()
                .setInputCols(new String[]{"airlineIndex","monthIndex","day_of_the_weekIndex","sourceIndex","destinationIndex","dep_timeZoneIndex","arrival_timeZoneIndex"})
                .setOutputCols(new String[]{"airlineIndexEnc","monthIndexEnc","day_of_the_weekIndexEnc","sourceIndexEnc","destinationIndexEnc","dep_timeZoneIndexEnc","arrival_timeZoneIndexEnc"});

        VectorAssembler assembler=new VectorAssembler()
                .setInputCols(new String[]{"airlineIndexEnc","monthIndexEnc","day_of_the_weekIndexEnc","sourceIndexEnc","destinationIndexEnc","dep_timeZoneIndexEnc","arrival_timeZoneIndexEnc","duration","total_stops"})
                .setOutputCol("features");

        //MinMaxScaler scaler=new MinMaxScaler().setInputCol("features_assem").setOutputCol("features");

        DecisionTreeRegressor dt=new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("features");

        Pipeline pipeline=new Pipeline().setStages(new PipelineStage[]{airlineIndexer,monthIndexer,dayOfTheWeekIndexer,sourceIndexer,
                destinationIndexer,dep_timeZoneIndexer,arrival_timeZoneIndexer,encoder,assembler,dt});
//        pipeline.fit(trainingSet).transform(testSet).show();

        ParamMap[] paramGrid = new ParamGridBuilder().addGrid(dt.maxDepth(), new int[] {5}).build();

        CrossValidator cv=new CrossValidator().setEstimator(pipeline).setEvaluator(new RegressionEvaluator()).setEstimatorParamMaps(paramGrid).setNumFolds(3);

        CrossValidatorModel crossValidatorModel=cv.fit(trainingSet);

        Dataset<Row> predictions=crossValidatorModel.transform(testSet);

        predictions.show(5);
    }
}
