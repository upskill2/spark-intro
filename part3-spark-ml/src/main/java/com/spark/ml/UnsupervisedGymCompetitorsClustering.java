package com.spark.ml;

import org.apache.commons.math3.ml.clustering.evaluation.ClusterEvaluator;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnsupervisedGymCompetitorsClustering {

    public static void main (String[] args) {

        System.setProperty ("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger ("org.apache").setLevel (org.apache.log4j.Level.WARN);


        SparkSession spark = SparkSession.builder ()
                .appName ("Gym Competitors")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .master ("local[*]")
                .getOrCreate ();

        Dataset<Row> csvData = spark
                .read ()
                .option ("header", true)
                .option ("inferSchema", true)
                .csv ("part3-spark-ml/src/main/resources/GymCompetition.csv");

        StringIndexer genderIndexer = new StringIndexer ()
                .setInputCol ("Gender")
                .setOutputCol ("GenderIndex");
        csvData = genderIndexer.fit (csvData).transform (csvData);

        OneHotEncoder genderEncoder = new OneHotEncoder ()
                .setInputCol ("GenderIndex")
                .setOutputCol ("GenderVector");

        csvData = genderEncoder.fit (csvData).transform (csvData);

        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"Age", "Height", "Weight", "GenderVector", "NoOfReps"})
                .setOutputCol ("features");

        csvData = vectorAssembler.transform (csvData).select ("features");

        KMeans kMeans = new KMeans ()
                .setK (5)
                .setSeed (1L);

        KMeansModel model = kMeans.fit (csvData);
        Dataset<Row> predictions = model.transform (csvData);

        final Vector[] vectors = model.clusterCenters ();
        for (Vector vector : vectors) {
            System.out.println (vector);
        }

        predictions.groupBy ("prediction").count ().show ();
        ClusteringEvaluator evaluator = new ClusteringEvaluator ()
                .setFeaturesCol ("features")
                .setPredictionCol ("prediction");

        spark.close ();

    }
}
