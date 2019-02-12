package com.zcx.config;

import com.zcx.spark.ClusteringExecutor;
import com.zcx.spark.MovieLensUserData;
import com.zcx.spark.RecommendationExecutor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/10/11  16:49
 */
@Configuration
public class BeanConfig {
    @Autowired
    private transient JavaSparkContext javaSparkContext;

    @Autowired
    private transient SparkSession sparkSession;
   /* //private SparkSession spark;

    @Bean
    JavaSparkContext javaSparkContext() {
        //spark = SparkSession.builder().master("local").appName("MovieLens").getOrCreate();
        //javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        SparkConf conf = new SparkConf().setAppName("MovieLens").setMaster("local[2]");
        javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setCheckpointDir("D:\\Checkpoint");
        return javaSparkContext;
    }*/

    @Bean
    MovieLensUserData movieLensUserData() {
        return new MovieLensUserData(javaSparkContext, "D:\\ml-100k/u.data");
    }

    @Bean
    RecommendationExecutor recommendationExecutorFromFile() {
        // 将文件转换为RDD集合  每行为一个String对象
        // 转换RDD集合  将String对象拆分为String数组

        //javaSparkContext读取
//        JavaRDD<String> originData = javaSparkContext.textFile("D:\\ml-1m/ratings.dat");
//        JavaRDD<String[]> rateData = originData.map(line -> line.split("::"));

        //sparkSession读取
        return new RecommendationExecutor("::", "D:\\ml-1m/ratings.dat");
    }

    @Bean
    RecommendationExecutor recommendationExecutorFromSQL() {
        Map<String, String> options = new HashMap();
        options.put("url", "jdbc:mysql://127.0.0.1/movielens2?useUnicode=true&characterEncoding=UTF-8");
        options.put("dbtable", "rate");
        options.put("user", "root");
        options.put("password", "sa123");

        Map<String, String> options2 = new HashMap();
        options2.put("url", "jdbc:mysql://127.0.0.1/movielens3?useUnicode=true&characterEncoding=UTF-8");
        options2.put("dbtable", "rate");
        options2.put("user", "root");
        options2.put("password", "sa123");

        return new RecommendationExecutor(options, options2);
    }

    @Bean
    ClusteringExecutor clusteringExecutorFromFile() {
        JavaRDD<String> lines = javaSparkContext.textFile("D:\\ml-100k/u.genre");
        //JavaRDD<String> lines = spark.read().textFile("D:\\ml-100k/u.genre").javaRDD();
        JavaRDD<String[]> genreData = lines.map(line -> line.split("\\|"));
        return new ClusteringExecutor(genreData);
    }
}
