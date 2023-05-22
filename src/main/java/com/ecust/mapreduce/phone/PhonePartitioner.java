package com.ecust.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        switch (text.toString().substring(0, 3)) {
            case "135":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
