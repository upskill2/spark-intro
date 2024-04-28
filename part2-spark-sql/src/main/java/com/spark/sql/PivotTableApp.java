package com.spark.sql;


import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class PivotTableApp {

    private static final Logger log = Logger.getLogger (PivotTableApp.class);


    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");


        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();


        Object[] months = new Object[]{"January", "February", "March", "April", "May", "June", "July", "August","Augcember", "September", "October",
                "November", "December"};
        List<Object> columns = Arrays.asList (months);

        session.read ().option ("header", "true")
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part2-spark-sql\\src\\main\\resources\\biglog.txt")
                .select (
                        col ("level"),
                        date_format (col ("datetime"), "MMMM").as ("month"),
                        date_format (col ("datetime"), "M").cast (DataTypes.IntegerType).alias ("monthnum"))
                .groupBy (col ("level"))
                .pivot ("month", columns)
                .count ()
                .na ()
                .fill (0)
                .show ();


        log.error ("-------------------------------");
        session.stop ();
    }

}
