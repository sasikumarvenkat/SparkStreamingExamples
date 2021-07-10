package com.demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaSparkStreamingExample {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("SparkStreamingExample").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 8000);

		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the
		// console
		wordCounts.print();
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
		jssc.close();
	}
}
