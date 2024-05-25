package com.henryhung.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class QueryRankingByTime {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Query Ranking By Time").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("assets/processed/search_data.sample/part-r-00000");
        JavaPairRDD<String, Integer> queryCounts = data
                .mapToPair(line -> {
                    String[] parts = line.split("\t");
                    String time = parts[0];
                    Integer count = Integer.parseInt(parts[2]);
                    return new Tuple2<>(time, count);
                })
                .reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swappedQueryCounts = queryCounts.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, String> sortedQueryCounts = swappedQueryCounts.sortByKey(false);

        System.out.println("Top 10 busy time periods:");
        sortedQueryCounts.take(10).forEach(result -> System.out.println("(" + result._2 + "," + result._1 + ")"));

        sc.close();
    }
}