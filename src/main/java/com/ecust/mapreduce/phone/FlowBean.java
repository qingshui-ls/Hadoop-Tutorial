package com.ecust.mapreduce.phone;

import com.sun.corba.se.impl.oa.toa.TOA;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
1。定义实现Writable 接口
2。重写序列化和反序列化方法
3。添加空参构造器
4。重写toString方法
* */
public class FlowBean implements WritableComparable<FlowBean> {
    //    上行流量 下行流量 总流量
    private Long upFlow;
    private Long downFlow;
    private Long totalFlow;

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowBean flowBean = (FlowBean) o;
        return Objects.equals(upFlow, flowBean.upFlow) && Objects.equals(downFlow, flowBean.downFlow) && Objects.equals(totalFlow, flowBean.totalFlow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(upFlow, downFlow, totalFlow);
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow() {
        this.totalFlow = this.upFlow + this.downFlow;
    }

    public FlowBean() {
        super();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.totalFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.totalFlow = dataInput.readLong();
    }


    @Override
    public int compareTo(FlowBean o) {
        // 总流量倒序
        int c = -Long.compare(this.totalFlow, o.getTotalFlow());
        // 总流量相同；按上行流量顺序排列 （二次排序）
        return c == 0 ? Long.compare(this.upFlow, o.getUpFlow()) : c;
    }
}
