package com.ecust.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
KEYIN: reduce阶段输入的key的类型：LongWritable
VALUEIN：reduce阶段输入value的类型：Text
KEYOUT：reduce阶段输出的key类型：Text
VALUEOUT：reduce阶段输出的value类型：IntWritable/LongWritable
* */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final Text text = new Text();
    private final LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // atguigu,(1,1)
        long sum = 0L;
        values.iterator();
        for (LongWritable l : values) {
            sum += l.get();
        }

        // 写出
        text.set(key);
        longWritable.set(sum);
        context.write(text, longWritable);
    }
}
