package com.spark.learning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class BasicLambda {

    private static final Logger logger = Logger.getLogger (BasicLambda.class);

    public static void main (String[] args) {

        List<Double> inputData = new ArrayList<> ();
        inputData.add (1.0);
        inputData.add (2.4545);
        inputData.add (3.12);
        inputData.add (50.52);

        Logger.getLogger ("org.apache").setLevel (Level.WARN);

        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);

        final JavaRDD<Double> rdd = sc.parallelize (inputData);
        rdd.reduce ((x, y) -> {
            final double v = x + y;
            logger.info ("----------------------");
            logger.info ("Adding " + x + " and " + y + " to get " + v);
            logger.info ("----------------------");
            return v;
        });


        logger.info ("----------------------");
        rdd.map (Math::sqrt)
                .foreach (value -> {
                    logger.info ("sqr is: " + value);
                });
        logger.info ("----------------------");

        //Count with map and reduce
        Long count = rdd.map (val -> 1L)
                .reduce ((x, y) -> {
                    final long v = x + y;
                    logger.info ("Counting: " + v);
                    return v;
                });
        logger.info ("----------------------");
        logger.info ("Count is: " + count);
        logger.info ("----------------------");
        sc.close ();
    }
}
