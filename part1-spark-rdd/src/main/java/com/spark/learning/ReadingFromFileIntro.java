package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ReadingFromFileIntro {

    public final static Logger logger = Logger.getLogger (ReadingFromFileIntro.class);

    private static final String PATH =
            "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\subtitles\\input-spring.txt";

    public static void main (String[] args) {
        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);


        final JavaRDD<String> intitialRDD = sc.textFile (PATH);

        final JavaRDD<String> words = intitialRDD.flatMap (sentence -> {
            return List.of (sentence.split (" ")).iterator ();
        });

        words.foreach (word -> {
            logger.info (word);
        });

        sc.close ();


    }
}
