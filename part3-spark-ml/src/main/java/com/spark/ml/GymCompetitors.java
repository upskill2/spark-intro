package com.spark.ml;

import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors {

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
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\GymCompetition.csv");

        StringIndexer genderIndexer = new StringIndexer ()
                .setInputCol ("Gender")
                .setOutputCol ("GenderIndex");

        csvData = genderIndexer.fit (csvData).transform (csvData);

        OneHotEncoder genderEncoder = new OneHotEncoder ()
                .setInputCol ("GenderIndex")
                .setOutputCol ("GenderVector");

        csvData = genderEncoder.fit (csvData).transform (csvData);


        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"Age", "Height", "Weight", "GenderVector"})
                .setOutputCol ("features");
        Dataset<Row> csvDataWithFeature = vectorAssembler.transform (csvData);

        csvDataWithFeature = csvDataWithFeature
                .select ("NoOfReps", "features")
                .withColumnRenamed ("NoOfReps", "label");


        final LinearRegressionModel model = new LinearRegression ()
                .fit (csvDataWithFeature);
        System.out.println ("The model has intercept " + model.intercept () + " and coefficients " + model.coefficients ());

        model.transform (csvDataWithFeature).show ();

        spark.close ();

    }
}
