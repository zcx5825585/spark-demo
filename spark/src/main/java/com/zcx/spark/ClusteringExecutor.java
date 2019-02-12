package com.zcx.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/10/13  15:23
 */
@Lazy
@Component
public class ClusteringExecutor {

    @Resource(name = "javaSparkContext")
    private JavaSparkContext sc;
    //  userID  filmID  rate
    //  196	    242	    3       ......
    private JavaRDD<String[]> genreData;

    //<用户，电影，评分>
    private MatrixFactorizationModel data;

    //聚类后的user特征集合
    private KMeansModel userModel;

    //聚类后的film特征集合
    private KMeansModel filmModel;

    private JavaRDD<Vector> userVectors;

    private JavaRDD<Vector> filmVectors;

    public ClusteringExecutor(String regex, String path) {
        JavaRDD<String> lines = sc.textFile(path);
        //JavaRDD<String> lines = spark.read().textFile("D:\\ml-100k/u.genre").javaRDD();
        JavaRDD<String[]> genreData = lines.map(line -> line.split(regex));
        this.genreData = genreData;
    }

    public ClusteringExecutor(JavaRDD<String[]> genreData) {
        this.genreData = genreData;
    }

    public void setData(MatrixFactorizationModel data) {
        this.data = data;
    }

    //返回KMeansModel模型 若未训练则开始训练并在完成返回模型
    public KMeansModel getUserModel() {
        if (userModel == null) {
            userModel = trainUser();
        }
        return userModel;
    }

    public KMeansModel getFilmModel() {
        if (filmModel == null) {
            filmModel = trainFilm();
        }
        return filmModel;
    }

    //重新开始训练 完成后返回用户的KMeansModel模型
    public KMeansModel getNewUserModel() {
        userModel = trainUser();
        return userModel;
    }

    public KMeansModel getNewFilmModel() {
        filmModel = trainFilm();
        return filmModel;
    }

    //获取model （获取特征 特征向量化 归一化验证 归一化处理 训练）
    private KMeansModel trainUser() {
        System.out.println("开始训练");
        /*获取用户相似特征*/
        JavaPairRDD<Object, double[]> userFactors = data.userFeatures().toJavaRDD().mapToPair(x -> x);
        /*用户特征向量化*/
        userVectors = userFactors.map(userFactor -> Vectors.dense(userFactor._2));

        //归一化验证？？
        RowMatrix userMatrix = new RowMatrix(userVectors.rdd());
        MultivariateStatisticalSummary userMatrixSummary = userMatrix.computeColumnSummaryStatistics();
        //System.out.println(userMatrixSummary.mean());//每列的平均值
        //System.out.println(userMatrixSummary.variance());//每列的方差

        //归一化处理

        //对用户因子进行kMeans聚类
        KMeansModel userModel = KMeans.train(userVectors.rdd(), 5, 100);//5个中心 最大100次迭代

        System.out.println("训练完成");
        return userModel;
    }

    public KMeansModel trainFilm() {
        System.out.println("开始训练");
        /*获取电影相似特征*/
        JavaPairRDD<Object, double[]> filmFactors = data.productFeatures().toJavaRDD().mapToPair(x -> x);
        /*电影特征向量化*/
        filmVectors = filmFactors.map(userFactor -> Vectors.dense(userFactor._2));

        //归一化验证？？
        RowMatrix filmMatrix = new RowMatrix(filmVectors.rdd());
        MultivariateStatisticalSummary filmMatrixSummary = filmMatrix.computeColumnSummaryStatistics();
        System.out.println(filmMatrixSummary.mean());//每列的平均值
        System.out.println(filmMatrixSummary.variance());//每列的方差

        //归一化处理

        //对电影因子进行kMeans聚类
        KMeansModel filmModel = KMeans.train(filmVectors.rdd(), 5, 100);

        System.out.println("训练完成");
        return filmModel;
    }

    //对原数据的聚类结果 <分类名称，元素数量 >
    public Map clusteringUsers() {
        System.out.println("用户聚类");
        JavaRDD<Integer> countByK = userModel.predict(this.userVectors);
        return countByK.countByValue();
    }

    public Map clusteringFilms() {
        System.out.println("电影聚类");
        JavaRDD<Integer> countByK = filmModel.predict(this.filmVectors);
        return countByK.countByValue();
    }

    //验证
    public double testUser() {
        System.out.println("验证电影聚类");
        double userCost = userModel.computeCost(userVectors.rdd());
        return userCost;
    }

    public double testFilm() {
        System.out.println("验证电影聚类");
        double filmCost = filmModel.computeCost(filmVectors.rdd());
        return filmCost;
    }
    //单个聚类结果

    //分类(RDD集合)

    //分类用户(单个)
    //参数为用户对所有当前电影的评分
    public Integer classifyUser(double[] rates) {
        return filmModel.predict(Vectors.dense(rates));
    }

    //分类电影(单个)
    //参数为当前所有用户对电影的评分
    public Integer classifyFilm(double[] rates) {
        return userModel.predict(Vectors.dense(rates));
    }
}
