package com.ecust.mapreduce.outputFormatTest;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {
        // 创建两条流
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            atguigu = fs.create(new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\o5\\atguigu.log"));
            other = fs.create(new Path("F:\\大数据\\尚硅谷\\hadoop\\output\\o5\\other.log"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        // 写流程
        String log = text.toString();
        if (log.contains("atguigu")) {
            atguigu.writeBytes(log + "\n");
        } else {
            other.writeBytes(log + "\n");
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关闭流
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
