package com.demo;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class TruckSparkStructuredStreaming {

	public static void main(String[] args) throws InterruptedException, StreamingQueryException {
		SparkConf conf = new SparkConf().setAppName("Truck-Spark-Structured-Streaming").setMaster("local[*]");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> truckinfoDf = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
				.option("subscribe", "truck-info-kafka")
				.load()
				.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
				.select(get_json_object(col("value"), "$.vehicleId").alias("vehicleId"),
						get_json_object(col("value"), "$.vehicleType").alias("vehicleType"),
						get_json_object(col("value"), "$.sensor1").alias("sensor1"),
						get_json_object(col("value"), "$.sensor2").alias("sensor2"),
						get_json_object(col("value"), "$.sensor3").alias("sensor3"),
						get_json_object(col("value"), "$.speed").alias("speed"),
						get_json_object(col("value"), "$.location.lat").alias("latitude"),
						get_json_object(col("value"), "$.location.lon").alias("longitude"));

		Dataset<Row> busInfo = truckinfoDf.filter(col("vehicleType").equalTo("Bus"));
		busInfo.writeStream().format("json")
				.option("checkpointLocation",
						"/Users/s0v00eo/demo-workspace/GenerateStreamData/simulated-data/check-point")
				.option("path", "/Users/s0v00eo/demo-workspace/GenerateStreamData/simulated-data/out/bus").start()
				.awaitTermination();

	}
}
