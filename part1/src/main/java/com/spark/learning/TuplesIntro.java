package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TuplesIntro {

    private static final Logger logger = Logger.getLogger (BasicLambda.class);

    public static void main (String[] args) {
        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);


        List<Double> inputData = new ArrayList<> ();
        inputData.add (1.0);
        inputData.add (2.4545);
        inputData.add (3.12);
        inputData.add (50.52);

        JavaRDD<Double> rdd = sc.parallelize (inputData);

        // Tuples
        JavaRDD<Tuple2<Double, Double>> result = rdd.map (value -> new Tuple2<> (value, Math.sqrt (value)));

        sc.close ();
    }
}
