package com.zcx.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 类说明
 *
 * @author zcx
 * @version 创建时间：2018/9/26  9:25
 */
public class ReduceTest extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //用迭代器的写法

        //建立一个values对象的迭代器,<>中的是泛型
        Iterator<LongWritable> iter = values.iterator();
        long sum = 0;
        //取出迭代器中的值
        while (iter.hasNext()) {
            sum += iter.next().get();
        }
        context.write(key, new LongWritable(sum));
        //用foreach的写法
        /*for(LongWritable value:values){

            sum += value.get();
        }*/

    }
}
