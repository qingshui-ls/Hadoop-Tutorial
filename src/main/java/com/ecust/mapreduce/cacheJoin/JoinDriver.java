package com.ecust.mapreduce.cacheJoin;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class JoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinDriver.class);
        job.addCacheFile(new URI("file:///F:/大数据/尚硅谷/hadoop/input/cacheJoin/pd.txt"));


        job.setMapperClass(JoinMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\input\\joinInput"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\OutJoin4"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
