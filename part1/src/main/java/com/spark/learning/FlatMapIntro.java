package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapIntro {
    public static final Logger logger = Logger.getLogger (FlatMapIntro.class);

    public static void main (String[] args) {

        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);

        List<String> inputData = new ArrayList<> ();
        inputData.add ("WARN: Tuesday 4 September 0405");
        inputData.add ("ERROR: Tuesday 4 September 0408");
        inputData.add ("FATAL: Wednesday 5 September 1632");
        inputData.add ("ERROR: Friday 7 September 1854");
        inputData.add ("WARN: Saturday 8 September 1942");
        inputData.add ("ERROR: Thursday 6 September 1942");

        final JavaRDD<String> parallelize = sc.parallelize (inputData);
        final JavaRDD<String> stringJavaRDD = parallelize.flatMap (value -> {
            return Arrays.asList (value.split (" ")).iterator ();
        }).filter (word -> word.length () > 1);

        logger.info ("----------------------");
        stringJavaRDD.foreach (value -> {
            logger.info (value);

        });
        logger.info ("----------------------");
        sc.close ();
    }
}
