package com.zcx;

import com.zcx.scala.ScalaTest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkApplication {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "d:\\hadoop/hadoop-2.6.5");
        SpringApplication.run(SparkApplication.class, args);
        System.out.println("start");
        ScalaTest.print();
    }
}
