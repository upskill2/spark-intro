package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class DStreamLogging {

    public static void main (String[] args) throws InterruptedException {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");

        SparkConf conf = new SparkConf ()
                .setAppName ("DStreamDemo")
                .setMaster ("local[*]");

        JavaStreamingContext jssc = new JavaStreamingContext (conf, Durations.seconds (2));


        jssc.socketTextStream ("localhost", 8989)
                .map (line -> line.split (",")[0])
                .mapToPair (level -> new Tuple2<> (level, 1))
                .reduceByKeyAndWindow ((a, b) -> a + b, Durations.seconds (40))
                .print ();

        jssc.start ();
        jssc.awaitTermination ();
    }

}
