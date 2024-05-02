package com.spark.ml;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
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
import scala.collection.Seq;

import java.util.HashSet;
import java.util.Set;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class MovieViewersCaseStudy {

    public static void main (String[] args) {


        SparkSession spark = SparkSession.builder ()
                .appName ("Movie Viewers")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .master ("local[*]")
                .getOrCreate ();

        Dataset<Row> csvData = spark
                .read ()
                .option ("header", true)
                .option ("inferSchema", true)
                .csv (
                        "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\part-r-00000-d55d9fed-7427-4d23-aa42-495275510f78.csv",
                        "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\part-r-00001-d55d9fed-7427-4d23-aa42-495275510f78.csv",
                        "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\part-r-00002-d55d9fed-7427-4d23-aa42-495275510f78.csv",
                        "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\part-r-00003-d55d9fed-7427-4d23-aa42-495275510f78.csv"
                )
                .filter (col ("is_cancelled").equalTo (false))
                .withColumn ("firstSub", when (col ("firstSub").isNull (), 0).otherwise (col ("firstSub")))
                .withColumn ("all_time_views", when (col ("all_time_views").isNull (), 0).otherwise (col ("all_time_views")))
                .withColumn ("last_month_views", when (col ("last_month_views").isNull (), 0).otherwise (col ("last_month_views")))
                .withColumn ("next_month_views", when (col ("next_month_views").isNull (), 0).otherwise (col ("next_month_views")))
                .drop ("observation_date", "is_cancelled")
                .withColumnRenamed ("next_month_views", "label");

        final Dataset<Row>[] dataSplits = csvData.randomSplit (new double[]{0.9, 0.1});
        final Dataset<Row> trainingAndTestData = dataSplits[0];
        final Dataset<Row> holdOutData = dataSplits[1];

        StringIndexer stringIndexer = new StringIndexer ()
                .setInputCols (new String[]{"payment_method_type", "country", "rebill_period_in_months"})
                .setOutputCols (new String[]{"paymentIndex", "countryIndex", "rebillIndex"});

        OneHotEncoder oneHotEncoder = new OneHotEncoder ()
                .setInputCols (new String[]{"paymentIndex", "countryIndex", "rebillIndex"})
                .setOutputCols (new String[]{"paymentVector", "countryVector", "rebillVector"});


        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"firstSub", "age", "all_time_views", "last_month_views", "paymentVector", "countryVector", "rebillVector"})
                .setOutputCol ("features");


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
                .setTrainRatio (0.9);

        Pipeline pipeline = new Pipeline ()
                .setStages (new PipelineStage[]{stringIndexer, oneHotEncoder, vectorAssembler, trainValidationSplit});

        PipelineModel pipelineModel = pipeline.fit (trainingAndTestData);
        TrainValidationSplitModel trainModel = (TrainValidationSplitModel) pipelineModel.stages ()[3];
        LinearRegressionModel model = (LinearRegressionModel) trainModel.bestModel ();

        System.out.println ("R2 " + model.summary ().r2 () + " RMSE " + model.summary ().rootMeanSquaredError ());

        Dataset<Row> holdOutResults = pipelineModel.transform (holdOutData);
        holdOutResults.show ();

    }
}
