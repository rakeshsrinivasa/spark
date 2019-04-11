package com.rsl.wordcount;



import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.INFO);
    SparkConf sparkConf = new SparkConf();

    //SetUp Spark Configuration
    sparkConf.setAppName("FirstWordCount");
    sparkConf.setMaster("local[*]");

    //Set Up Java Spark Context
    JavaSparkContext sc =  new JavaSparkContext(sparkConf);

    //Read the Input files

    JavaRDD<String> lines = sc.textFile("input/wordcount.text");
    JavaRDD<String> words =  lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    Map<String,Long> wordCounts = words.countByValue();

    for(Map.Entry<String, Long> entry : wordCounts.entrySet()) {
      System.out.println(entry.getKey() + " : " + entry.getValue() );
    }



  }
}
