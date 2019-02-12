package com.zcx.streaming;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/11/7  15:35
 */
public class Streaming {
    private JavaStreamingContext ssc;

    public Streaming() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        ssc = new JavaStreamingContext("local[2]","test", Durations.seconds(6));
    }


    public void startRead() throws InterruptedException {
//        Collection<String> topics = Arrays.asList("topicA");
//        JavaInputDStream<ConsumerRecord<String, String>> lines =KafkaUtils.createDirectStream(
//                ssc,
//                LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//        );

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.map(line -> line.split(" ")[0]);
        words.print();
        JavaPairDStream<String, Integer> wordsWithOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordsWithCount = wordsWithOne.reduceByKey((v1, v2) -> v1 + v2);
        wordsWithCount.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
