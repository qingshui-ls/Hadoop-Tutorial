package com.ecust.mapreduce.combineText;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
KEYIN: map阶段输入的key的类型：LongWritable
VALUEIN：map阶段输入value的类型：Text
KEYOUT：map阶段输出的key类型：Text
VALUEOUT：map阶段输出的value类型：IntWritable/LongWritable
* */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text outKey = new Text();
    private final LongWritable outValue = new LongWritable(1L);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // 1.获取一行
        String line = value.toString();
        // 2.切割
        String[] words = line.split(" ");
        // 3.循环写出
        for (String s : words) {

            outKey.set(s);
            context.write(outKey, outValue);
        }

    }
}

