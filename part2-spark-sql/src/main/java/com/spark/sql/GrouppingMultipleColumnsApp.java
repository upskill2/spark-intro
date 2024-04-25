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

public class GrouppingMultipleColumnsApp {

    private static final Logger log = Logger.getLogger (GrouppingMultipleColumnsApp.class);

    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");


        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();


        session.read ().option ("header", "true")
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part2-spark-sql\\src\\main\\resources\\biglog.txt")
                .createOrReplaceTempView ("biglog_table");


        session.sql ("select level, date_format(datetime, 'MMMM') as month, count(1) " +
                "as total,  date_format(datetime, 'M')   as monthnum " +
                "from biglog_table group by level, month, date_format(datetime, 'M') order by cast (date_format(datetime, 'M') as int) desc, level asc")
                .drop ("monthnum")
                .show (10);

        session.stop ();
    }

}
