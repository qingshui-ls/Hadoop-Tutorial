package com.ecust.mapreduce.mapJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, LongWritable, TableBean> {
    private final TableBean tableBean = new TableBean();
    private String fileName;
    private final LongWritable outKey = new LongWritable();

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, TableBean>.Context context) throws IOException, InterruptedException {
        // 初始化order pd 获取filename名称
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, TableBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if ("order.txt".equals(this.fileName)) {
            tableBean.setPid(Long.parseLong(split[1]));
            tableBean.setCount(Long.parseLong(split[2]));
            tableBean.setOrderId(Long.parseLong(split[0]));
            tableBean.setpName("");
            tableBean.setTableName("order");
        } else {
            tableBean.setPid(Long.parseLong(split[0]));
            tableBean.setpName(split[1]);
            tableBean.setTableName("pd");
            tableBean.setCount(0L);
            tableBean.setOrderId(-1L);
        }
        outKey.set(tableBean.getPid());
        context.write(outKey, tableBean);
    }
}
