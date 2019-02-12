package com.zcx.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/9/26  9:19
 */
public class MapTest extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取整行
        String str = value.toString();
        //进行切分
        String[] words = str.split(" ");

        //输出
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }

}
