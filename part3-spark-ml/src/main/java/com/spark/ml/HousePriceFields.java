package com.spark.ml;

import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceFields {

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
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part3-spark-ml\\src\\main\\resources\\kc_house_data.csv");

        csvData = csvData.drop ("id", "date", "waterfront", "view", "condition", "grade",
                "yr_renovated", "lat", "long", "sqft_living15", "sqft_lot15", "zipcode", "sqft_lot", "yr_built");

        for (String col : csvData.columns ()) {
            for (String col1 : csvData.columns ()) {
                System.out.println ("Column name is: " + col + " and " + col1 + ": " + csvData.stat ().corr (col, col1));
            }
        }
        System.out.println ("-------------------------------");

        for (String col : csvData.columns ()) {
                System.out.println ("Column name is: " + col + " and coor: " + csvData.stat ().corr (col, "price"));
        }


        VectorAssembler vectorAssembler = new VectorAssembler ()
                .setInputCols (new String[]{"bedrooms", "bathrooms", "sqft_living", "floors",
                        "sqft_above", "sqft_basement", "sqft_living15", "sqft_lot15"})
                .setOutputCol ("features");


    }

}
