package com.ecust.mapreduce.combineText;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2.设置jar报路径
        job.setJarByClass(WordCountDriver.class);
        // 3.设置mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4.设置map输出的kv值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 5.设置最终的输出kv值
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置CombineFileInputFormat,处理多个小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\inputword"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\o1"));

        // 7.提交job
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
