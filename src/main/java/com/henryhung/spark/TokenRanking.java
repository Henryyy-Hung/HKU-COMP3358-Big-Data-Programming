package com.henryhung.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;

public class TokenRanking {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Token Ranking").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("assets/processed/search_data.sample/part-r-00000");
        JavaPairRDD<String, Integer> tokens = data
                .flatMap(line -> Arrays.asList(line.split("\t")[1].split("\\.")).iterator())
                .mapToPair(token -> new Tuple2<>(token, 1))
                .reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swappedTokenCounts = tokens.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, String> sortedTokens = swappedTokenCounts.sortByKey(false);

        System.out.println("Top 10 tokens:");
        sortedTokens.take(10).forEach(result -> System.out.println("(" + result._2 + "," + result._1 + ")"));

        sc.close();
    }
}