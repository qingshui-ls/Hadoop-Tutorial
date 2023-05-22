package com.ecust.mapreduce.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable l : values) {
            sum += l.get();
        }
        longWritable.set(sum);
        context.write(key, longWritable);
    }
}
