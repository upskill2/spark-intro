package com.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DStreamViewingFigures {
    public static void main (String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf ().setAppName ("DStreamViewingFigures").setMaster ("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext (conf, Durations.seconds (3));


        Map<String, Object> params = new HashMap<> ();
        params.put ("bootstrap.servers", "localhost:9092");
        params.put ("key.deserializer", StringDeserializer.class);
        params.put ("value.deserializer", StringDeserializer.class);
        params.put ("group.id", "spark-group");
        params.put ("auto.offset.reset", "latest");
        params.put ("enable.auto.commit", false);


        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream (jssc, LocationStrategies.PreferConsistent (),
                ConsumerStrategies.Subscribe (Arrays.asList ("viewrecords"), params));

        stream.map (ConsumerRecord::value)
                .mapToPair (record -> new Tuple2<> (record, 1L))
                .reduceByKeyAndWindow ((a, b) -> a + b, Durations.seconds (6), Durations.seconds (3))
                .mapToPair (Tuple2::swap)
                .transformToPair (rdd -> rdd.sortByKey (false))
                .print ();

        jssc.start ();

        jssc.awaitTermination ();
    }
}
