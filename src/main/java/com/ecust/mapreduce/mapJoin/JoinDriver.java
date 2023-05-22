package com.ecust.mapreduce.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinDriver.class);

        job.setReducerClass(JoinReducer.class);
        job.setMapperClass(JoinMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TableBean.class);

        FileInputFormat.setInputPaths(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\input\\joinInput"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\OutJoin3"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
