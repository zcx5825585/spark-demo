package com.zcx.controller;

import com.zcx.spark.RecommendationExecutor;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * 推荐
 *
 * @author zcx
 * @version 创建时间：2018/10/11  16:24
 */
@RestController
@RequestMapping("/recommendation")
public class RecommendationController {
    @Resource(name = "recommendationExecutorFromFile")
    //@Resource(name = "recommendationExecutorFromSQL")
    private RecommendationExecutor recommendationExecutor;

    //获得模型
    @RequestMapping("getModel")
    public String getModel() {
        MatrixFactorizationModel model = recommendationExecutor.getModel();
        return model.toString();
    }

    //重新训练模型
    @RequestMapping("getNewModel")
    public String getNewModel() {
        MatrixFactorizationModel model = recommendationExecutor.getNewModel();
        return model.toString();
    }


    //预测评分
    @RequestMapping("makePoint")
    public double makePoint(Integer userId, Integer filmID) {
        return recommendationExecutor.makePoint(userId, filmID);
    }

    //推荐电影
    @RequestMapping("recommendation")
    public List<String> recommendation(Integer userId, Integer count) {
        List<String> result = new ArrayList<>();
        Rating[] ratings = recommendationExecutor.recommendation(userId, count);
        for (Rating rating : ratings) {
            result.add(rating.toString());
        }
        return result;
    }

    //验证
    @RequestMapping("test")
    public String test(String testPath) {
        return recommendationExecutor.test() + "\n" + recommendationExecutor.testSelf();
    }

    //试参
    @RequestMapping("best")
    public void best() {
        recommendationExecutor.best();
    }

    /*
    @RequestMapping("rateStatistics")
    public String rateStatistics() {
        return recommendationExecutor.statistics();
    }

    @RequestMapping("userRateCount")
    public Map<Integer, Integer> userRateCount() {
        return recommendationExecutor.userRateCount();
    }
*/
}
