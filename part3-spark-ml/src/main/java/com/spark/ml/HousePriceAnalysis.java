package com.spark.ml;

import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePriceAnalysis {

    public static void main (String[] args) {

        System.setProperty ("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger ("org.apache").setLevel (org.apache.log4j.Level.WARN);


        SparkSession spark = SparkSession.builder ()
                .appName ("House Prices")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .master ("local[*]")
                .getOrCreate ();


        Dataset<Row> csvData = spark
                .read ()
                .option ("header", true)
                .option ("inferSchema", true)
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\kc_house_data.csv")
                .withColumn ("sqft_above_percentage", col ("sqft_above").divide (col ("sqft_living")))
                .withColumnRenamed ("price", "label");

        final Dataset<Row>[] dataSplits = csvData.randomSplit (new double[]{0.8, 0.2});
        final Dataset<Row> trainingAndTestData = dataSplits[0];
        final Dataset<Row> holdOutData = dataSplits[1];

        StringIndexer stringIndexer = new StringIndexer ()
                .setInputCols (new String[]{"condition", "grade", "zipcode"})
                .setOutputCols (new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"});

        //   csvData = stringIndexer.fit (csvData).transform (csvData);

        OneHotEncoder oneHotEncoder = new OneHotEncoder ()
                .setInputCols (new String[]{"conditionIndex", "gradeIndex", "zipcodeIndex"})
                .setOutputCols (new String[]{"conditionVector", "gradeVector", "zipcodeVector"});

        //    csvData = oneHotEncoder.fit (csvData).transform (csvData);

        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"bedrooms", "bathrooms", "sqft_living", "floors", "sqft_above_percentage",
                        "conditionVector", "gradeVector", "zipcodeVector", "waterfront"})
                .setOutputCol ("features");
/*
        csvData = vectorAssembler
                .transform (csvData)
                .select ("label", "features");*/

        LinearRegression linearRegression = new LinearRegression ();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder ();
        ParamMap[] paramMap = paramGridBuilder
                .addGrid (linearRegression.regParam (), new double[]{0.01, 0.1, 0.5})
                .addGrid (linearRegression.elasticNetParam (), new double[]{0, 0.5, 1})
                .build ();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit ()
                .setEstimator (linearRegression)
                .setEvaluator (new RegressionEvaluator ()
                        .setMetricName ("r2"))
                .setEstimatorParamMaps (paramMap)
                .setTrainRatio (0.8);
/*
        LinearRegressionModel model = (LinearRegressionModel) trainValidationSplit
                .fit (trainingAndTestData)
                .bestModel ();*/

        Pipeline pipeline = new Pipeline ()
                .setStages (new PipelineStage[]{stringIndexer, oneHotEncoder, vectorAssembler, trainValidationSplit});

        PipelineModel pipelineModel = pipeline.fit (trainingAndTestData);
        TrainValidationSplitModel trainModel = (TrainValidationSplitModel) pipelineModel.stages ()[3];
        LinearRegressionModel model = (LinearRegressionModel) trainModel.bestModel ();

        Dataset<Row> holdOutResults = pipelineModel.transform (holdOutData);
        holdOutResults.show ();
        holdOutResults = holdOutResults.drop ("prediction");

        System.out.println ("R2 " + model.summary ().r2 () + " RMSE " + model.summary ().rootMeanSquaredError ());

/*        model.transform (holdOutResults)
                .show ();*/
        System.out.println ("R2 " + model.evaluate (holdOutResults).r2 () + " RMSE " + model.evaluate (holdOutResults).rootMeanSquaredError ());
        System.out.println ("coeficients " + model.coefficients () + " and intercept " + model.intercept ());
        System.out.println ("regParam " + model.getRegParam () + " elasticNetParam " + model.getElasticNetParam ());

    }
}
