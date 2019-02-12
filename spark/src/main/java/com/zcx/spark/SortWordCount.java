package com.zcx.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

@Component
public class SortWordCount implements Serializable {
    public void count() {
        //System.setProperty("hadoop.home.dir", "f:\\hadoop/hadoop-2.6.5");
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建lines RDD
        JavaRDD<String> lines = sc.textFile("D:/uv.txt");

        //Map<String, ?> resultMap=wordPair.countByKey();
        Map<String, ?> resultMap = count(lines);

        for (String key : resultMap.keySet()) {
            System.out.println("word \"" + key + "\" appears " + resultMap.get(key) + " times.");
        }
        sc.close();
    }

    public Map<String, Integer> count(JavaRDD<String> lines) {
        //对wordPair 进行按键计数
        return lines.map(rdd->rdd.split(" ")[0])
                .mapToPair(rdd->new Tuple2<>(rdd,1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(rdd -> new Tuple2<>(rdd._2, rdd._1))
                .sortByKey(false)
                .mapToPair(rdd -> new Tuple2<>(rdd._2, rdd._1))
                .collectAsMap();
        // 到这里为止，就得到了每个单词出现的次数
        // 我们的新需求，是要按照每个单词出现次数的顺序，降序排序
        // wordCounts RDD内的元素是这种格式：(spark, 3) (hadoop, 2)
        // 因此我们需要将RDD转换成(3, spark) (2, hadoop)的这种格式，才能根据单词出现次数进行排序
        // 进行key-value的反转映射
        // 按照key进行排序
        // 再次将value-key进行反转映射
       // 到此为止，我们获得了按照单词出现次数排序后的单词计数
        //sortedWordCount.foreach(rdd-> System.out.println("word \"" + rdd._1 + "\" appears " + rdd._2 + " times."));

    }
}
