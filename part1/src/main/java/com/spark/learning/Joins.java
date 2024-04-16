package com.spark.learning;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.List;

import static com.spark.learning.util.Util.isBoring;

public class Joins {

    private static final Logger logger = Logger.getLogger (Joins.class);
    private final static String PATH =
            "C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\subtitles\\input-spring.txt";

    public static void main (String[] args) {

        SparkConf conf = new SparkConf ().setAppName ("spark-learning").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);

        List<Tuple2<Integer, Integer>> visitsRaw = List.of (
                new Tuple2<> (4, 18),
                new Tuple2<> (6, 4),
                new Tuple2<> (99, 9)
  /*             , new Tuple2<> (10, 10),
                new Tuple2<> (10, 11),
                new Tuple2<> (18, 2)*/
        );

        List<Tuple2<Integer, String>> nameRaw = List.of (
                new Tuple2<> (1, "John"),
                new Tuple2<> (2, "Jane"),
                new Tuple2<> (3, "Jack"),
                new Tuple2<> (4, "Jill"),
                new Tuple2<> (5, "James"),
                new Tuple2<> (6, "Brad"),
                new Tuple2<> (7, "Jesse"),
                new Tuple2<> (8, "Jenny"),
                new Tuple2<> (9, "Jared"),
                new Tuple2<> (10, "Josh"),
                new Tuple2<> (11, "Jill"),
                new Tuple2<> (12, "Jen"),
                new Tuple2<> (13, "Jesse"),
                new Tuple2<> (14, "Jenny"),
                new Tuple2<> (15, "Jared"),
                new Tuple2<> (16, "Josh"),
                new Tuple2<> (17, "Jill"),
                new Tuple2<> (18, "Jen")
        );

        final JavaPairRDD<Integer, Integer> visit = sc.parallelizePairs (visitsRaw);
        final JavaPairRDD<Integer, String> users = sc.parallelizePairs (nameRaw);


        //final JavaPairRDD<Integer, Tuple2<Integer, String>> join = visit.join (users);
        final JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> join = visit.leftOuterJoin (users);

        join.foreach (tuple -> {
            logger.info (tuple + " times");
        });
        sc.close ();

    }
}
