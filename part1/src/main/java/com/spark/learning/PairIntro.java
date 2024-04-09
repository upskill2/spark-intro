package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairIntro {

    public final static Logger logger = Logger.getLogger (PairIntro.class);

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

        final JavaPairRDD<String, Long> pairRDD = parallelize.mapToPair (raw -> {
            final String[] split = raw.split (":");
            return new Tuple2<> (split[0], 1L);
        }).reduceByKey ((x, y) -> x + y);

        pairRDD.foreach (tuple -> {
            logger.info("----------------------");
            logger.info (tuple._1 + " has " + tuple._2 + " instances");
            logger.info("----------------------");
        });

        sc.close ();


    }
}
