package com.zcx.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/10/25  15:17
 */
@Configuration
public class SparkContextBean {
    private String appName = "MovieLens";

    private String master = "local[4]";

    private String checkpoint = "D:\\Checkpoint\\MovieLens";

    @Bean
    public SparkSession sparkSession() throws Exception {
        SparkSession ss = SparkSession.builder().appName(appName).master(master).getOrCreate();
        return ss;
    }

    @Bean
    public JavaSparkContext javaSparkContext() throws Exception {
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession().sparkContext());
        javaSparkContext.setCheckpointDir(checkpoint);
        return javaSparkContext;
    }

}
