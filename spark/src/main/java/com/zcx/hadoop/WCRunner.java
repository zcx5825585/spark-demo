package com.zcx.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 类说明
 * <p>
 * Class clazz=(Class<T>)((ParameterizedType)(Type)getClass()).getActualTypeArguments()[0];
 *
 * @author zcx
 * @version 创建时间：2018/9/14  17:54
 */
public class WCRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置文件
        Configuration conf = new Configuration();
        //获取一个作业
        Job job = Job.getInstance(conf);

        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(WCRunner.class);
        //本job使用的mapper和reducer的类
        job.setMapperClass(MapTest.class);
        job.setReducerClass(ReduceTest.class);
        //指定reduce的输出数据key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定mapper的输出数据key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定要处理的输入数据存放路径
        FileInputFormat.setInputPaths(job, new Path("C:/Users/i/Desktop/in/"));
        //指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/i/Desktop/out/"));
        //将job提交给集群运行
        job.waitForCompletion(true);
    }
}
