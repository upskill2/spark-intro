package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Pattern;

import static com.spark.learning.util.Util.isBoring;

public class TopWords {

    private static final Logger logger = Logger.getLogger (TopWords.class);
    private final static String PATH =
            "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\subtitles\\input-spring.txt";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);

        sc.textFile (PATH)
                .flatMap (sentence -> List.of (sentence.split (" "))
                        .iterator ())
                .filter (word -> word.length () > 1)
                .map (word -> word.replaceAll ("\\W", ""))
                .filter (word -> word.length () > 0)
                .map (word -> word.toLowerCase ())
                .filter (word -> !isBoring (word))
                .filter (word -> !word.matches ("\\d+"))
                .mapToPair (word -> new Tuple2<> (word, 1L))
                .reduceByKey ((value1, value2) -> value1 + value2)
                .mapToPair (Tuple2::swap)
                .sortByKey (false)
                //   .getNumPartitions ()
                .take (10)
                //  .foreach (logger::info);
                .forEach (logger::info);

     //   System.out.println (numPartitions);

        //   longStringJavaPairRDD =
/*        sc.textFile (PATH)
                .flatMap (sentence -> List.of (sentence.split (" "))
                        .iterator ())
                .filter (word -> word.length () > 1)
                .map (word -> word.replaceAll ("\\W", ""))
                .filter (word -> word.length () > 0)
                .map (word -> word.toLowerCase ())
                .filter (word -> !isBoring (word))
                .filter (word -> !word.matches ("\\d+"))
                .mapToPair (word -> new Tuple2<> (word, 1L))
                .reduceByKey ((value1, value2) -> value1 + value2)
                .mapToPair (Tuple2::swap)
                .sortByKey (false)
                .coalesce (1)
                .foreach (logger::info);*/

        //    longStringJavaPairRDD.foreach (logger::info);

        String s = "the";
        final boolean boring = isBoring (s);
        System.out.println (boring);

        sc.close ();

    }
}
