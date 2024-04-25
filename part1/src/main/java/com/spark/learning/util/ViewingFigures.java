package com.spark.learning.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures {
    @SuppressWarnings ("resource")
    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger ("org.apache").setLevel (Level.WARN);

        SparkConf conf = new SparkConf ().setAppName ("startingSpark").setMaster ("local[*]");
        JavaSparkContext sc = new JavaSparkContext (conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd (sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd (sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd (sc, testMode);

        //warm up
/*        final List<Tuple2<Integer, Integer>> collect = chapterData
                .mapToPair (Tuple2::swap)
                .mapToPair (word -> new Tuple2<> (word._1, 1))
                .reduceByKey ((x, y) -> x + y)
                .collect ();*/
        // TODO - over to you!

        //Step 1 - remove any duplicate views
        final JavaPairRDD<Integer, Integer> removingDupes = viewData.distinct ()
                ;
    //    final List<Tuple2<Integer, Integer>> checkDupesRemoved = removingDupes.collect ();

        //Step 2 - Joining to get Course Id
        final JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinChapterAndCourse = removingDupes
                .mapToPair (Tuple2::swap).join (chapterData.distinct ());
  //      final List<Tuple2<Integer, Tuple2<Integer, Integer>>> checkJoin = joinChapterAndCourse.collect ();

        //Step 3 - Drop the Chapter Id
        final JavaPairRDD<Tuple2<Integer, Integer>, Integer> dropChapter = joinChapterAndCourse
                .mapToPair (tuple -> new Tuple2<> (tuple._2, 1));
     //   final List<Tuple2<Tuple2<Integer, Integer>, Integer>> checkDrop = dropChapter.collect ();


        //Step 4 - Count views
        final JavaPairRDD<Tuple2<Integer, Integer>, Integer> userAndViews = dropChapter.reduceByKey (Integer::sum);
     //   final List<Tuple2<Tuple2<Integer, Integer>, Integer>> checkUsersAndViews = userAndViews.collect ();

        //Step 5 - Drop the user
        final JavaPairRDD<Integer, Integer> dropUser = userAndViews
                .mapToPair (tuple -> new Tuple2<> (tuple._1._2, tuple._2));
   //     final List<Tuple2<Integer, Integer>> checkDropUser = dropUser.collect ();


        //Step 6 - Of how many chapters
        final JavaPairRDD<Integer, Integer> numberOfCourseinChapter = chapterData
                .distinct ()
                .mapToPair (tuple -> new Tuple2<> (tuple._2, 1))
                .reduceByKey (Integer::sum);
 ////       final List<Tuple2<Integer, Integer>> checkTotalNumberOfCourses = numberOfCourseinChapter.collect ();

        final JavaPairRDD<Integer, Tuple2<Integer, Integer>> ofHowManyChapters = dropUser.
                leftOuterJoin (numberOfCourseinChapter)
                .mapToPair (tuple -> new Tuple2<> (tuple._1, new Tuple2<> (tuple._2._1, tuple._2._2 ().orElse (1))));
  //      final List<Tuple2<Integer, Tuple2<Integer, Integer>>> checkOfHowMany = ofHowManyChapters.collect ();

        //Step 7  - convert to percentage
        final JavaPairRDD<Integer, Double> percenatge = ofHowManyChapters
                .mapToPair (tuple -> new Tuple2<Integer, Double> (tuple._1,
                        Double.valueOf (tuple._2._1) / Double.valueOf (tuple._2._2)
                        //(Optional.ofNullable (tuple._2._2).orElse (1)
                ));
   //     final List<Tuple2<Integer, Double>> checkPercentage = percenatge.collect ();

        //Step 8 - Convert to Scores
        final JavaPairRDD<Integer, Integer> mapToScores = percenatge.mapValues (v -> {
            if (v >= 0.9) {
                return 10;
            } else if (v >= 0.5 && v < 0.9) {
                return 4;
            } else if (v >= 0.25 && v < 0.5) {
                return 2;
            }
            return 0;
        });
  //      final List<Tuple2<Integer, Integer>> listMapToScore = mapToScores.collect ();

        //Step 9 - Add up scores
        final JavaPairRDD<Integer, Integer> addUpScores = mapToScores.reduceByKey ((x, y) -> x + y);
  //      final List<Tuple2<Integer, Integer>> listAddUp = addUpScores.collect ();

        //Exercise 3
        final JavaPairRDD<String, Integer> addNames = titlesData.leftOuterJoin (addUpScores)
                .mapToPair (tuple -> new Tuple2<> (tuple._2._1, tuple._2._2.orElse (-1)))
                .sortByKey (false);

        addNames.persist (StorageLevel.MEMORY_AND_DISK_SER ());

        final List<Tuple2<String, Integer>> listAddnames = addNames.collect ();
        addNames.take (10).forEach (System.out::println);

        Scanner scanner = new Scanner (System.in);
        scanner.nextLine ();

        sc.close ();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd (JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<> ();
            rawTitles.add (new Tuple2<> (1, "How to find a better job"));
            rawTitles.add (new Tuple2<> (2, "Work faster harder smarter until you drop"));
            rawTitles.add (new Tuple2<> (3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs (rawTitles);
        }
        return sc.textFile ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\viewing figures\\titles.csv")
                .mapToPair (commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split (",");
                    return new Tuple2<Integer, String> (Integer.valueOf (cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd (JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<> ();
            rawChapterData.add (new Tuple2<> (96, 1));
            rawChapterData.add (new Tuple2<> (96, 1));
            rawChapterData.add (new Tuple2<> (97, 1));
            rawChapterData.add (new Tuple2<> (98, 1));
            rawChapterData.add (new Tuple2<> (99, 2));
            rawChapterData.add (new Tuple2<> (100, 3));
            rawChapterData.add (new Tuple2<> (101, 3));
            rawChapterData.add (new Tuple2<> (102, 3));
            rawChapterData.add (new Tuple2<> (103, 3));
            rawChapterData.add (new Tuple2<> (104, 3));
            rawChapterData.add (new Tuple2<> (105, 3));
            rawChapterData.add (new Tuple2<> (106, 3));
            rawChapterData.add (new Tuple2<> (107, 3));
            rawChapterData.add (new Tuple2<> (108, 3));
            rawChapterData.add (new Tuple2<> (109, 3));
            return sc.parallelizePairs (rawChapterData);
        }

        return sc.textFile ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\viewing figures\\chapters.csv")
                .mapToPair (commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split (",");
                    return new Tuple2<Integer, Integer> (Integer.valueOf (cols[0]), Integer.valueOf (cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd (JavaSparkContext sc, boolean testMode) {

        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<> ();
            rawViewData.add (new Tuple2<> (14, 96));
            rawViewData.add (new Tuple2<> (14, 97));
            rawViewData.add (new Tuple2<> (13, 96));
            rawViewData.add (new Tuple2<> (13, 96));
            rawViewData.add (new Tuple2<> (13, 96));
            rawViewData.add (new Tuple2<> (14, 99));
            rawViewData.add (new Tuple2<> (13, 100));
            return sc.parallelizePairs (rawViewData);
        }

        return sc.textFile ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part1\\src\\main\\resources\\viewing figures\\views-*.csv")
                .mapToPair (commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split (",");
                    return new Tuple2<Integer, Integer> (Integer.valueOf (columns[0]), Integer.valueOf ((columns[1])));
                });
    }
}
