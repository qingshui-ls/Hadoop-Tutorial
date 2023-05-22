package com.ecust.mapreduce.outputFormatTest;

import com.ecust.mapreduce.wordcount.WordCountDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);

        // 设置自定义OutputFormat，继承自FileOutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\input\\accesslog"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\o5"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

    }
}
