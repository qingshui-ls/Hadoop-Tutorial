package com.ecust.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        Long downFlowTotal = 0L;
        Long upFlowTotal = 0L;
        for (FlowBean f : values) {
            downFlowTotal += f.getDownFlow();
            upFlowTotal += f.getUpFlow();
        }
        flowBean.setUpFlow(upFlowTotal);
        flowBean.setDownFlow(downFlowTotal);
        flowBean.setTotalFlow();
        context.write(key, flowBean);
    }
}
