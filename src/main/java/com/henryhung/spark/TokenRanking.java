package com.henryhung.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TokenRanking {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Token Ranking").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("assets/processed/search_data.sample/part-r-00000");
        JavaPairRDD<String, Integer> tokens = data
                .flatMapToPair(line -> {
                    String[] parts = line.split("\t");
                    String domain = parts[1];
                    Integer count = Integer.parseInt(parts[2]);
                    List<String> tokenList = Arrays.asList(domain.split("\\."));
                    return tokenList.stream()
                            .map(token -> new Tuple2<>(token, count))
                            .collect(Collectors.toList())
                            .iterator();
                })
                .reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swappedTokenCounts = tokens.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, String> sortedTokens = swappedTokenCounts.sortByKey(false);

        System.out.println("Top 10 tokens:");
        sortedTokens.take(10).forEach(result -> System.out.println("(" + result._2 + "," + result._1 + ")"));

        sc.close();
    }
}