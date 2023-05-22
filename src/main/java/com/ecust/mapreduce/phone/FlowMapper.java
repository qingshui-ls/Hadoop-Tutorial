package com.ecust.mapreduce.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private final FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        Text outKey = new Text(split[1]);
        Long upFlow = Long.parseLong(split[split.length - 3]);
        Long downFlow = Long.parseLong(split[split.length - 2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setTotalFlow();
        context.write(outKey, flowBean);

    }
}
