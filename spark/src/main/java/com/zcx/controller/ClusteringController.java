package com.zcx.controller;

import com.zcx.spark.ClusteringExecutor;
import com.zcx.spark.RecommendationExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/10/11  16:24
 */
@RestController
@RequestMapping("/clustering")
public class ClusteringController {
    @Resource(name = "recommendationExecutorFromFile")
    private RecommendationExecutor recommendationExecutor;

    @Resource(name = "clusteringExecutorFromFile")
    private ClusteringExecutor clusteringExecutor;


    @RequestMapping("getUserModel")
    public String getUserModel() {
        clusteringExecutor.setData(recommendationExecutor.getModel());
        return clusteringExecutor.getUserModel().toString();
    }

    @RequestMapping("getFilmModel")
    public String getFilmModel() {
        clusteringExecutor.setData(recommendationExecutor.getModel());
        return clusteringExecutor.getFilmModel().toString();
    }

    @RequestMapping("clusteringUsers")
    public Map clusteringUsers() {
        return clusteringExecutor.clusteringUsers();
    }

    @RequestMapping("clusteringFilms")
    public Map clusteringFilms() {
        return clusteringExecutor.clusteringFilms();
    }

    @RequestMapping("testUser")
    public double testUser() {
        return clusteringExecutor.testUser();
    }

    @RequestMapping("testFilm")
    public double testFilm() {
        return clusteringExecutor.testFilm();
    }

//    //读取sql数据
//    @RequestMapping("sqltest")
//    public void sqltest() {
//        //创建SQLContext
//        SQLContext sql = SQLContext.getOrCreate(javaSparkContext.sc());
//        //从数据库读取数据
//        Dataset dataset = sql.read().jdbc("jdbc:mysql://localhost:3306/test?user=root&password=sa123", "rate", new Properties());
//        ALS als=new ALS().setMaxIter(5).setRegParam(0.01);
//        ALSModel model=als.fit(dataset);
//    }
}
