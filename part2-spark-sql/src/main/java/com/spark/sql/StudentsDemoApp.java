package com.spark.sql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class StudentsDemoApp {

    private static final Logger log = Logger.getLogger (StudentsDemoApp.class);

    public static void main (String[] args) {
        System.setProperty ("hadoop.home.dir", "c:/hadoop");
        //    Logger.getLogger ("org.apache").setLevel (Level.INFO);


        SparkSession session = SparkSession.builder ()
                .appName ("Students Demo App")
                .master ("local[*]")
                .config ("spark.sql.warehouse.dir", "file:///c:/tmp1/")
                .getOrCreate ();

        Dataset<Row> dataset = session
                .read ()
                .option ("header", "true")
                .csv ("C:\\Users\\taras.chmeruk\\IdeaProjects\\spark-intro\\part2-spark-sql\\src\\main\\resources\\exams\\students.csv");
        log.info ("--------------------");
        //  log.info (String.valueOf (dataset.count ()));
        log.info ("--------------------");

/*        final Row first = dataset.first ();
        final String string = first.get (2).toString ();
        System.out.println (string);
        final int year = Integer.parseInt ( first.getAs ("year"));*/
        //    dataset.show ();

        //   Dataset<Row> math = dataset.filter ("subject = 'Math' AND year > 2007");
        //math.show ();
        dataset.createOrReplaceTempView ("students_table");
        session.sql ("SELECT distinct(year) FROM students_table WHERE subject = 'French' AND year > 2007 order by year");

        List<Row> inMemory = new ArrayList<> ();
        StructField[] fields = new StructField[]{
                new StructField ("level", DataTypes.StringType, false, Metadata.empty ()),
                new StructField ("datetime", DataTypes.StringType, false, Metadata.empty ())
        };

        inMemory.add (RowFactory.create ("WARN", "2016-12-31 05:19:54"));
        inMemory.add (RowFactory.create ("FATAL", "2016-12-31 04:19:12"));
        inMemory.add (RowFactory.create ("ERROR", "2016-12-31 04:19:33"));
        inMemory.add (RowFactory.create ("INFO", "2016-12-31 06:19:41"));
        inMemory.add (RowFactory.create ("WARN", "2016-12-31 04:19:00"));
        final Dataset<Row> dataFrame = session.createDataFrame (inMemory, new StructType (fields));
        dataFrame.show ();





/*        final Dataset<Row> mathCase2 = dataset.filter (dataset.col ("subject").equalTo ("Math").and (dataset.col ("year").gt (2007)));
        mathCase2.first ();*/

/*        Column subject = col ("subject");
        Column year = col ("year");
        dataset.filter (subject.equalTo ("Math").and (year.geq (2008))).show ();*/

        session.stop ();
    }

}
