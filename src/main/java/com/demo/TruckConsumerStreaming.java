package com.demo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class TruckConsumerStreaming {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("TruckInfo-Streaming").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "truck-info-consumer-group");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("truck-info-kafka");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaPairDStream<String, String> records = stream
				.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

		records.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
