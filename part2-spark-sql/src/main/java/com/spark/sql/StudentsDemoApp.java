package com.spark.sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class StudentsDemoApp {
    @SuppressWarnings ("resource")
    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger ("org.apache").setLevel (Level.WARN);


        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();



        session
                .read ()
                .csv ("src/main/resources/exams/students.csv")//C:\Users\taras.chmeruk\IdeaProjects\spark-intro\part2-spark-sql\
                .createOrReplaceTempView ("students");

        session.close ();
    }

}
