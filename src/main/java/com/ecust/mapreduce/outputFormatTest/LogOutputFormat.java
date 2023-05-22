package com.ecust.mapreduce.outputFormatTest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new LogRecordWriter(taskAttemptContext);
    }
}
