package com.spark.sql;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class JavaGrouppingMultipleColumnsApp {

    private static final Logger log = Logger.getLogger (JavaGrouppingMultipleColumnsApp.class);

    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");


        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();


        session.read ().option ("header", "true")
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part2-spark-sql\\src\\main\\resources\\biglog.txt")
                //        .selectExpr ("level", "date_format( datetime, 'MMMM') as month")
                .select (col ("level"), date_format (col ("datetime"), "MMMM").as ("month"))
                .show ();

        session.stop ();
    }

}
