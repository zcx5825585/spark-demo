package com.zcx.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

@Component
public class MovieLensUserData implements Serializable {

    private JavaRDD<String> lines;

    private JavaRDD<String[]> userData;

//    public MovieLensUserData() {
//        //System.setProperty("hadoop.home.dir", "f:\\hadoop/hadoop-2.6.5");
//        SparkConf conf = new SparkConf().setAppName("MovieLensUserData").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        // 创建lines RDD
//        initData("G:\\ml-100k/u.user");
//    }

    public MovieLensUserData(JavaSparkContext sc, String path) {
        // 将文件转换为RDD集合  每行为一个String对象
        lines = sc.textFile(path);
        // 转换RDD集合  将String对象拆分为String数组
        userData = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split("\\|");
            }
        });

    }

    public String countUserData() {
        // 统计用户数
        JavaRDD<String> userCountRDD = userData.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[0];
            }
        });
        long userCount = userCountRDD.count();

        // 统计性别数

        JavaRDD<String> genderCountRDD = userData.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[2];
            }
        });
        long genderCount = genderCountRDD.distinct().count();

        //统计职业数

        JavaRDD<String> occupationCountRDD = userData.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[3];
            }
        });
        long occupationCount = occupationCountRDD.distinct().count();

        //统计邮编数

        JavaRDD<String> zipcodeCountRDD = userData.map(new Function<String[], String>() {
            @Override
            public String call(String[] strings) throws Exception {
                return strings[4];
            }
        });
        long zipcodeCount = zipcodeCountRDD.distinct().count();

        return "用户数:" + userCount + ", 性别数:" + genderCount + ", 职业数:" + occupationCount + ", 邮编数:" + zipcodeCount;
    }

    public Map<Integer, Long> ageAuto() {
        JavaRDD<Integer> age = userData.map((String[] s) -> Integer.parseInt(s[1]));
        age = age.sortBy(x -> x, true, 0);
        return age.countByValue();
    }

    public Map<Integer, Integer> age() {

        //扩充列数
        JavaPairRDD<Integer, Integer> ageWithTime = userData.mapToPair(new PairFunction<String[], Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String[] s) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.parseInt(s[1]), 1);
            }
        });

        //根据key进行分组 每组循环使用call方法得到该组的唯一结果
        // 3个泛型为 key 原数据value 结果value
        JavaPairRDD<Integer, Integer> ageWithCount = ageWithTime.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;//相同Key的2条数据value相加
            }
        });
        ageWithCount = ageWithCount.sortByKey(true);
        return ageWithCount.collectAsMap();
    }
}
