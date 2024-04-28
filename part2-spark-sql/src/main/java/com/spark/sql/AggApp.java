package com.spark.sql;


import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class AggApp {

    private static final Logger log = Logger.getLogger (AggApp.class);

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


        dataset.groupBy ("subject")
                .pivot ("year")
                .count ()
                .agg (
                        max (col ("score").cast (DataTypes.IntegerType)).alias ("max_score"),
                        min (col ("score").cast (DataTypes.IntegerType)).alias ("min_score"),
                        avg (col ("score").cast (DataTypes.IntegerType)).alias ("avg_score")
                )
                .show ();

        session.stop ();
    }

}
