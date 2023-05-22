package com.ecust.mapreduce.comparablePhone;

import com.ecust.mapreduce.phone.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private final FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        flowBean.setUpFlow(Long.parseLong(split[1]));
        flowBean.setDownFlow(Long.parseLong(split[2]));
        flowBean.setTotalFlow();
        context.write(flowBean, new Text(split[0]));
    }
}
