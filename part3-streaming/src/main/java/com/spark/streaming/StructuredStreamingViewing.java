package com.spark.streaming;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class StructuredStreamingViewing {

    public static void main (String[] args) throws TimeoutException, StreamingQueryException {
     //   System.setProperty ("hadoop.home.dir", "c:/hadoop");

        SparkSession spark = SparkSession.builder ()
                .appName ("StructuredStreamingViewing")
                .master ("local[*]")
            //    .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();

        spark.readStream ()
                .format ("kafka")
                .option ("kafka.bootstrap.servers", "localhost:9092")
                .option ("subscribe", "viewrecords")
                .load ()
                .createOrReplaceTempView ("viewrecords_figures");
               /* .selectExpr ("CAST(value AS STRING)")
                .writeStream ()
                .format ("console")
                .start ()
                .awaitTermination ();*/

        spark.sql ("SELECT value FROM viewrecords_figures")
                .writeStream ()
                .format ("console")
                .outputMode (OutputMode.Append ())
                .start ()
                .awaitTermination ();

    }
}
