package com.zcx.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import javax.annotation.Resource;
import java.io.File;
import java.io.Serializable;
import java.util.Map;

@Lazy
@Component
public class RecommendationExecutor implements Serializable {
    //  userID  filmID  rate
    //  196	    242	    3       ......
    private JavaRDD<String[]> rateData;

    private JavaRDD<String[]> testData;

    private MatrixFactorizationModel model;

    @Resource(name = "javaSparkContext")
    private JavaSparkContext sc;

    private SparkSession sparkSession = SparkSession.builder().getOrCreate();

    public RecommendationExecutor(String regex, String path) {
        String[] paths = path.split(",");
        Dataset<String> rate = sparkSession.read().textFile(paths[0]);
        for (int i = 1; i < paths.length; i++) {
            rate = rate.union(sparkSession.read().textFile(paths[i]));
        }
        JavaRDD<String[]> rateData = rate.javaRDD().map(line -> line.split(regex));
        new RecommendationExecutor(rateData);
    }

    public RecommendationExecutor(Map<String, String>... options) {
        Dataset<Row> rate = sparkSession.read().format("jdbc").options(options[0]).load();
        for (int i = 1; i < options.length; i++) {
            rate = rate.union(sparkSession.read().format("jdbc").options(options[i]).load());
        }
        rate.createOrReplaceTempView("rate");
        sparkSession.sql("select * from rate").limit(5).show();
        sparkSession.sql("select count(*) from rate").show();
        JavaRDD<String[]> rateData = rate.javaRDD()
                .map(row -> new String[]{row.getString(0), row.getString(1), row.getString(2)});
        new RecommendationExecutor(rateData);
    }

    public RecommendationExecutor(JavaRDD<String[]> allData) {
        try {
            model = MatrixFactorizationModel.load(sc.sc(), "D:/out");
            System.out.println("加载模型成功");
        } catch (Exception e) {
            System.out.println("加载模型失败");
        }

        rateData = allData.sample(false, 0.8);

        testData = allData.subtract(rateData);
    }

    //返回MatrixFactorizationModel模型
    //若未训练则开始训练并在完成返回模型
    public MatrixFactorizationModel getModel() {
        if (model == null) {
            model = train();
        }
        return model;
    }

    //重新开始训练 完成后返回MatrixFactorizationModel模型
    //若未训练则开始训练 完成后返回模型
    public MatrixFactorizationModel getNewModel() {
        model = train();
        return model;
    }

    //-----------------------------------------关键方法--------------------------------------
    //训练得到MatrixFactorizationModel模型（所有用户对所有电影的评分）
    private MatrixFactorizationModel train() {
        //转换为<用户ID,影片ID，单次评分> 即Rating格式（已有的用户对电影的评分信息）
        JavaRDD<Rating> ratings = rateData.map(rateData -> new Rating(Integer.parseInt(rateData[0]), Integer.parseInt(rateData[1]), Double.parseDouble(rateData[2])));

        System.out.println("开始训练");
        //通过ALS算法 由Rating格式的数据训练得到MatrixFactorizationModel模型（所有用户对所有电影的评分）

        //100k数据  隐因子个数5  最大迭代20 正则化参数0.1  平均差约0.72  标准差约0.92　
        //10m数据  隐因子个数7  最大迭代25 正则化参数0.05 平均差约0.677  标准差约0858
        MatrixFactorizationModel model = ALS.train(ratings.rdd(), 7, 25, 0.05);

        System.out.println(model.predict(1, 1));
        System.out.println("训练完成");
        //saveModel(model);
        return model;
    }

    //---------------------------------------------------------------------------------------
    public void best() {
        int[] a = {7};
        int[] b = {25};
        double[] c = {0.05, 0.06};
        JavaRDD<Rating> ratings = rateData.map(rateData -> new Rating(Integer.parseInt(rateData[0]), Integer.parseInt(rateData[1]), Double.parseDouble(rateData[2])));
        JavaPairRDD<Integer, Integer> userFilmRDD = testData.mapToPair(s -> new Tuple2<>(Integer.parseInt(s[0]), Integer.parseInt(s[1])));
        JavaPairRDD<Tuple2<Integer, Integer>, Object> real = testData.mapToPair(rateData -> new Tuple2<>(new Tuple2<>(Integer.parseInt(rateData[0]), Integer.parseInt(rateData[1])), Double.parseDouble(rateData[2])));

        double min = 1;
        int r1 = 0;
        int r2 = 0;
        double r3 = 0;

        for (int v1 : a) {
            for (int v2 : b) {
                for (double v3 : c) {
                    model = train(ratings, v1, v2, v3);
                    double result = test2(userFilmRDD, real);
                    System.out.println(v1 + "  " + v2 + "  " + v3 + "  " + result);
                    if (min > result) {
                        min = result;
                        r1 = v1;
                        r2 = v2;
                        r3 = v3;
                    }
                }
            }
        }
        System.out.println("best");
        System.out.println(r1 + "  " + r2 + "  " + r3 + "  " + min);
    }

    private MatrixFactorizationModel train(JavaRDD<Rating> ratings, int var1, int var2, double var3) {
        //转换为<用户ID,影片ID，单次评分> 即Rating格式（已有的用户对电影的评分信息）

        //通过ALS算法 由Rating格式的数据训练得到MatrixFactorizationModel模型（所有用户对所有电影的评分）

        //10m数据
        // 5 20 0.1 平均差约0.67  标准差约0.84
        MatrixFactorizationModel model = ALS.train(ratings.rdd(), var1, var2, var3);

        //saveModel(model);
        return model;
    }

    public double test2(JavaPairRDD<Integer, Integer> userFilmRDD, JavaPairRDD<Tuple2<Integer, Integer>, Object> ratings) {

        // 创建<<用户id,影片id> ,预测评分>
        JavaPairRDD<Tuple2<Integer, Integer>, Object> predictions = model.predict(userFilmRDD).mapToPair(rating -> new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating()));


        // 通过join 获得 以<用户id,影片id> 为key ，<预测评分-实际评分> 为value的PairRDD
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Object, Object>> ratingsAndPredictionsWithKey = ratings.join(predictions);
        // 保留 <预测评分-实际评分>
        JavaRDD<Tuple2<Object, Object>> ratingsAndPredictions = ratingsAndPredictionsWithKey.map(withKey -> withKey._2);

        // 参数rdd为一组2列的数据 即 <预测数值，真实数值>    需要转换为标准RDD
        RegressionMetrics regressionMetrics = new RegressionMetrics(ratingsAndPredictions.rdd());

        return regressionMetrics.rootMeanSquaredError();
    }


    //通过模型预测userID对filmID的评分
    public double makePoint(Integer userID, Integer filmID) {
        System.out.println("评分预测");
        return model.predict(userID, filmID);
    }

    //通过模型为userID推荐最适合的count部电影
    public Rating[] recommendation(Integer userID, Integer count) {
        System.out.println("电影推荐");
        return model.recommendProducts(userID, count);
    }

    //利用RegressionMetrics验证预测准确率
    public String test() {
        //JavaRDD<String[]> testData = sc.textFile("G:\\ml-1m/test.dat").map(old -> old.split("::"));
        //JavaRDD<String[]> testData = sc.textFile("G:\\ml-1m/ratings.dat").map(line -> line.split("::"));
        return test(testData);
    }

    public String testSelf() {
        //JavaRDD<String[]> testData = sc.textFile("G:\\ml-1m/test.dat").map(old -> old.split("::"));
        //JavaRDD<String[]> testData = sc.textFile("G:\\ml-1m/ratings.dat").map(line -> line.split("::"));
        return test(rateData);
    }

    public String test(JavaRDD<String[]> testData) {

        System.out.println("开始验证");
        // 创建<用户id,影片id>
        JavaPairRDD<Integer, Integer> userFilmRDD = testData.mapToPair(s -> new Tuple2<>(Integer.parseInt(s[0]), Integer.parseInt(s[1])));
        // 创建<<用户id,影片id> ,预测评分>
        JavaPairRDD<Tuple2<Integer, Integer>, Object> predictions = model.predict(userFilmRDD).mapToPair(rating -> new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating()));

        // 创建<<用户id,影片id> ,实际评分>
        JavaPairRDD<Tuple2<Integer, Integer>, Object> ratings = testData.mapToPair(rateData -> new Tuple2<>(new Tuple2<>(Integer.parseInt(rateData[0]), Integer.parseInt(rateData[1])), Double.parseDouble(rateData[2])));

        // 通过join 获得 以<用户id,影片id> 为key ，<预测评分-实际评分> 为value的PairRDD
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Object, Object>> ratingsAndPredictionsWithKey = ratings.join(predictions);
        // 保留 <预测评分-实际评分>
        JavaRDD<Tuple2<Object, Object>> ratingsAndPredictions = ratingsAndPredictionsWithKey.map(withKey -> withKey._2);

        // 参数rdd为一组2列的数据 即 <预测数值，真实数值>    需要转换为标准RDD
        RegressionMetrics regressionMetrics = new RegressionMetrics(ratingsAndPredictions.rdd());

        String out = "平均差 \n " + regressionMetrics.meanAbsoluteError() + "\n" + "平均标准差（开方平均方差） \n " + regressionMetrics.rootMeanSquaredError();
        System.out.println("验证完成");
        System.out.println(out);
        return out;
    }


    //控制方法：重载数据 allData
    public void reLoadData(JavaRDD<String[]> allData) {
        rateData = allData.sample(false, 0.8);
        testData = allData.subtract(rateData);
    }


    //数据信息
    private String statistics() {
        JavaRDD<Integer> pointRDD = rateData.map(s -> Integer.parseInt(s[2]));
        Integer maxPoint = pointRDD.reduce((point1, point2) -> Math.max(point1, point2));
        Integer minPoint = pointRDD.reduce((point1, point2) -> Math.min(point1, point2));
        Integer sumPoint = pointRDD.reduce((point1, point2) -> point1 + point2);
        float count = pointRDD.count();
        return "平分数：" + rateData.count() + "最高分：" + maxPoint + "最低分：" + minPoint + "平均分：" + sumPoint / count;
    }

    //按userID计数
    private Map<Integer, Integer> userRateCount() {
        //转换为 <用户ID,单次评分>
        JavaPairRDD<Integer, Integer> userRate = rateData.mapToPair(new PairFunction<String[], Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String[] strings) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.parseInt(strings[0]), Integer.parseInt(strings[2]));
            }
        });
        //根据 用户ID 分组         得到 <用户ID,该用户评分集合>
        JavaPairRDD<Integer, Iterable<Integer>> rateByUserRDD = userRate.groupByKey();
        //转换为 <用户ID,该用户评分此数>
        JavaPairRDD<Integer, Integer> rateCountByuserRdd = rateByUserRDD.mapToPair(new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                int count = 0;
                for (Integer i : integerIterableTuple2._2()) {
                    count++;
                }
                return new Tuple2<>(integerIterableTuple2._1, count);
            }
        });
        //根据 用户ID 排序
        rateCountByuserRdd = rateCountByuserRdd.sortByKey(true);
        return rateCountByuserRdd.collectAsMap();
    }

    private void saveModel(MatrixFactorizationModel model, String path) {
        if (new File(path).exists()) {
            System.out.println("文件已存在");
        } else {
            model.save(sc.sc(), path);
            System.out.println("保存成功");
        }
    }

}
