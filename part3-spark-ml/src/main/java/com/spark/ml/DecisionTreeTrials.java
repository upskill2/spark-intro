package com.spark.ml;

import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DecisionTreeTrials {

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
                .csv ("part3-spark-ml/src/main/resources/vppFreeTrials.csv");

        spark.udf ().register ("countries", (String country) -> {
            switch (country) {
                case "US":
                    return "US";
                case "UK":
                    return "UK";
                case "IN":
                    return "India";
                case "BE":
                case "BG":
                case "CZ":
                case "DK":
                case "DE":
                case "EE":
                case "IE":
                case "EL":
                case "ES":
                case "FR":
                case "HR":
                case "IT":
                case "CY":
                case "LV":
                case "LT":
                case "LU":
                case "HU":
                case "MT":
                case "NL":
                case "AT":
                case "PL":
                case "PT":
                case "RO":
                case "SI":
                case "SK":
                case "FI":
                case "SE":
                case "CH":
                case "IS":
                case "NO":
                case "LI":
                case "EU":
                    return "EUROPE";
                default:
                    return "OTHER";
            }

        }, DataTypes.StringType);


        csvData = csvData
                .withColumn ("country", callUDF ("countries", col ("country")))
                .withColumn ("label", when (col ("payments_made").geq (1), lit (1)).otherwise (lit (0)))
        ;

        StringIndexer countryIndexer = new StringIndexer ()
                .setInputCol ("country")
                .setOutputCol ("countryIndex");

        csvData = countryIndexer.fit (csvData).transform (csvData);
        final Dataset<Row> countryIndex = csvData.select ("countryIndex").distinct ();

        new IndexToString ()
                .setInputCol ("countryIndex")
                .setOutputCol ("originalCountry")
                .transform (countryIndex);

        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"countryIndex", "rebill_period", "chapter_access_count", "seconds_watched"})
                .setOutputCol ("features");

        csvData = vectorAssembler.transform (csvData).select ("features", "label");

        Dataset<Row>[] dataSplits = csvData.randomSplit (new double[]{0.9, 0.1});
        Dataset<Row> trainingAndTestData = dataSplits[0];
        Dataset<Row> holdOutData = dataSplits[1];

        DecisionTreeClassifier regressor = new DecisionTreeClassifier ()
                .setMaxDepth (3);

        final DecisionTreeClassificationModel model = regressor.fit (trainingAndTestData);
        final Dataset<Row> predictions = model.transform (holdOutData);
        System.out.println (model.toDebugString ());

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator ()
                .setMetricName ("accuracy");
        System.out.println ("Accuracy is: " + evaluator.evaluate (predictions));

        RandomForestClassifier randomForestClassifier = new RandomForestClassifier ()
                .setNumTrees (10)
                .setMaxDepth (3);

        final RandomForestClassificationModel model2 = randomForestClassifier.fit (trainingAndTestData);
        final Dataset<Row> predictions2 = model2.transform (holdOutData);
        predictions2.show ();
        System.out.println (model2.toDebugString ());
        System.out.println ("Accuracy is: " + evaluator.evaluate (predictions2));

    }
}
