package com.ecust.mapreduce.comparablePhone;

import com.ecust.mapreduce.phone.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        switch (text.toString().substring(0, 3)) {
            case "135":
                return 0;
            case "137":
                return 1;
            default:
                return 2;
        }
    }
}
