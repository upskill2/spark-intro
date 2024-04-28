package com.spark.sql;


import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class PracticalSessionApp {

    private static final Logger log = Logger.getLogger (PracticalSessionApp.class);

    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");
        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();

        Dataset<Row> dataset = session
                .read ()
                .option ("header", "true")
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part2-spark-sql\\src\\main\\resources\\exams\\students.csv");

        dataset.groupBy (col ("subject"))
                .pivot (col ("year"))
                .agg (
                        round (avg (col ("score").cast ((DataTypes.DoubleType))), 2).alias ("avg"),
                        round (stddev (col ("score").cast (DataTypes.DoubleType)), 2).alias ("stddev")
                )
                .show ();


        session.stop ();
    }

}
