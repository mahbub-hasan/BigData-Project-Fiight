package model;

import data_preprocessing.Fly;
import data_preprocessing.Preparation;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import utilities.Constant;

import java.util.ArrayList;
import java.util.List;

public class KMeansWithRegression {
    public static final String toIntSuffix="ToInt";
    public static final String toVecSuffix="ToVec";
    public static final String[] featuresCols = {Constant.AIRLINE+toVecSuffix,Constant.MONTH+toVecSuffix,Constant.DAY_OF_THE_WEEK+toVecSuffix,Constant.SOURCE+toVecSuffix,Constant.DESTINATION+toVecSuffix,Constant.DEP_TIME_ZONE+toVecSuffix,Constant.ARRIVAL_TIME_ZONE+toVecSuffix,Constant.DURATION,Constant.TOTAL_STOPS};
    public static final String label = Constant.PRICE;
    public static final String featuresColumnName = "features";

    CrossValidator validator;
    CrossValidatorModel decisionTreeModel;

    ParamMap[] hyperParameter;
    ClusteringEvaluator evaluator;
    Pipeline estimator;

    StringIndexer textToInt;
    OneHotEncoder intToClassVector;
    VectorAssembler featureAssembler;

    LinearRegressionModel[] regressionByCluster;
    KMeans kmeansAlg;
    CrossValidatorModel kmeansModel;
    int k_clusters;
    public KMeansWithRegression(){

        textToInt=new StringIndexer()
                .setInputCols(new String[]{Constant.AIRLINE,Constant.MONTH,Constant.DAY_OF_THE_WEEK,Constant.SOURCE,Constant.DESTINATION,Constant.DEP_TIME_ZONE,Constant.ARRIVAL_TIME_ZONE})
                .setOutputCols(new String[]{Constant.AIRLINE+toIntSuffix,Constant.MONTH+toIntSuffix,Constant.DAY_OF_THE_WEEK+toIntSuffix,Constant.SOURCE+toIntSuffix,Constant.DESTINATION+toIntSuffix,Constant.DEP_TIME_ZONE+toIntSuffix,Constant.ARRIVAL_TIME_ZONE+toIntSuffix});

        intToClassVector=new OneHotEncoder()
                .setInputCols(new String[]{Constant.AIRLINE+toIntSuffix,Constant.MONTH+toIntSuffix,Constant.DAY_OF_THE_WEEK+toIntSuffix,Constant.SOURCE+toIntSuffix,Constant.DESTINATION+toIntSuffix,Constant.DEP_TIME_ZONE+toIntSuffix,Constant.ARRIVAL_TIME_ZONE+toIntSuffix})
                .setOutputCols(new String[]{Constant.AIRLINE+toVecSuffix,Constant.MONTH+toVecSuffix,Constant.DAY_OF_THE_WEEK+toVecSuffix,Constant.SOURCE+toVecSuffix,Constant.DESTINATION+toVecSuffix,Constant.DEP_TIME_ZONE+toVecSuffix,Constant.ARRIVAL_TIME_ZONE+toVecSuffix});

        featureAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol(featuresColumnName);


        kmeansAlg = new KMeans().setSeed(1L);


        estimator = new Pipeline().setStages(new PipelineStage[]{textToInt,intToClassVector,featureAssembler,kmeansAlg});
        evaluator = new ClusteringEvaluator();
        hyperParameter = new ParamGridBuilder().addGrid(kmeansAlg.k(), new int[] {2,4,8}).build();

        validator = new CrossValidator()
                .setEstimator(estimator)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(hyperParameter)
                .setNumFolds(5)
                .setSeed(1L);
    }

    public void fit(Dataset<Fly> data) {
        kmeansModel = validator.fit(data);
        ParamMap[] estimatorParamMaps = kmeansModel.getEstimatorParamMaps();

        double[] avgMetrics = kmeansModel.avgMetrics();
        for (int i = 0; i < avgMetrics.length; i++) {
            System.out.println(estimatorParamMaps[i]+" "+avgMetrics[i]);
        }
        Dataset<Row> clusteredData = kmeansModel.transform(data);
        clusteredData.persist();
        k_clusters=(int) clusteredData.select("prediction").distinct().as(Encoders.INT()).count();
        System.out.println("Cluster number: "+k_clusters);

        //applying linear regression
        regressionByCluster=new LinearRegressionModel[k_clusters];
        for (int i = 0; i < regressionByCluster.length; i++) {
            Dataset<Row> trainRegression = Preparation.getClusterInstance(i, "prediction", clusteredData).drop("prediction");
            LinearRegression model=new LinearRegression().setFeaturesCol(featuresColumnName).setLabelCol(label);
            regressionByCluster[i]=model.fit(trainRegression);
        }
        clusteredData.unpersist();
    }

    public List<Dataset<Row>> transform(Dataset<Fly> data){

        List<Dataset<Row>> partial = new ArrayList<Dataset<Row>>();
        Dataset<Row> clustered = kmeansModel.transform(data);
        clustered.cache();
        for (int i = 0; i < regressionByCluster.length; i++) {
            Dataset<Row> clusterInstances = Preparation.getClusterInstance(i, "prediction", clustered).drop("prediction");
            if(!clusterInstances.isEmpty()) {
                Dataset<Row> predicted = regressionByCluster[i].transform(clusterInstances);
                partial.add(predicted);
            }
        }
        clustered.unpersist();
        return partial;
    }
}
