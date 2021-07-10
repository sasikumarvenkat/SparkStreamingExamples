package com.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaSparkExample {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkExample").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> list = Arrays.asList(1, 2, 3, 45, 6, 7, 8, 9);

		JavaRDD<Integer> listRdd = sc.parallelize(list);

		System.out.println("Rdd Count:" + listRdd.count());

		JavaRDD<Integer> filteredRdd = listRdd.filter(rdd -> rdd % 2 == 0);

		System.out.println("Filtered Count:" + filteredRdd.count());

		sc.close();
	}

}