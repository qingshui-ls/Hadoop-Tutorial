package com.ecust.mapreduce.phone;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 自定义数据分区
        job.setPartitionerClass(PhonePartitioner.class);
        // 指定相应数量的reduceTask
        job.setNumReduceTasks(5);


//        6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\input\\inputphone"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\o3"));
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7.执行任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
