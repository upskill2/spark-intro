package com.spark.ml;

import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class UnsupervisedMatrixFactorization {

    private static ALS als;

    public static void main (String[] args) {

        System.setProperty ("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger ("org.apache").setLevel (org.apache.log4j.Level.WARN);


        SparkSession spark = SparkSession.builder ()
                .appName ("Matrix Factorization")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .master ("local[*]")
                .getOrCreate ();

        Dataset<Row> csvData = spark
                .read ()
                .option ("header", true)
                .option ("inferSchema", true)
                .csv ("part3-spark-ml/src/main/resources/VPPcourseViews.csv")
                .withColumn ("proportionWatched", col ("proportionWatched").multiply (100))
/*                .groupBy ("userId")
                .pivot ("courseId")
                .sum ("proportionWatched")*/
                ;


        final Dataset<Row>[] randomSplit = csvData.randomSplit (new double[]{0.8, 0.2});
        final Dataset<Row> trainingDataAndTestData = randomSplit[0];
        final Dataset<Row> testData = randomSplit[1];

        ALS als = new ALS ()
                .setMaxIter (10)
                .setRegParam (0.1)
                .setUserCol ("userId")
                .setItemCol ("courseId")
                .setRatingCol ("proportionWatched");

        final ALSModel model = als.fit (csvData);
        final List<Row> userRecomList = model.recommendForAllUsers (5).takeAsList (5);

        for (Row row : userRecomList) {
          int userId = row.getAs (0);
          String recommendations = row.getAs (1).toString ();

            System.out.println ("User " + userId + " recommendations: " + recommendations);
            System.out.println ("User also watched: ");
            csvData.filter (col ("userId").equalTo (userId)).show ();
        }


        spark.close ();

    }
}
