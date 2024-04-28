package com.spark.sql;


import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UDFApp {

    private static final Logger log = Logger.getLogger (UDFApp.class);

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

        session.udf ().register ("passed", (String grade, String subject) -> {
            if (subject.equals ("Biology")) {
                return grade.startsWith ("A");
            }
            return grade.startsWith ("A") || grade.startsWith ("B") || grade.startsWith ("C");
        }, DataTypes.BooleanType);


        dataset
                .withColumn ("pass", callUDF ("passed", col ("grade"), col ("subject")))
                .show ();


        session.stop ();
    }

}
