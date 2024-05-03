package com.spark.sql;


import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class GrouppingAndAggregationApp {

    private static final Logger log = Logger.getLogger (GrouppingAndAggregationApp.class);

    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");

        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();

        List<Row> inMemory = new ArrayList<> ();

        inMemory.add (RowFactory.create ("WARN", "2016-12-31 05:19:54"));
        inMemory.add (RowFactory.create ("FATAL", "2016-12-31 04:19:12"));
        inMemory.add (RowFactory.create ("ERROR", "2016-12-31 04:19:33"));
        inMemory.add (RowFactory.create ("INFO", "2016-12-31 06:19:41"));
        inMemory.add (RowFactory.create ("WARN", "2016-12-31 04:19:00"));

        StructField[] fields = new StructField[]{
                new StructField ("level", DataTypes.StringType, false, Metadata.empty ()),
                new StructField ("datetime", DataTypes.StringType, false, Metadata.empty ())
        };
        final Dataset<Row> dataFrame = session.createDataFrame (inMemory, new StructType (fields));
        dataFrame.createOrReplaceTempView ("logging_table");
        session.sql ("SELECT level, collect_set(datetime) FROM logging_table GROUP BY level").show ();


       session.stop ();
    }

}
