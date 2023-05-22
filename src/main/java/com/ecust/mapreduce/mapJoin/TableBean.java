package com.ecust.mapreduce.mapJoin;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements WritableComparable<TableBean> {

    private Long pid;
    private Long orderId;
    private Long count;
    private String pName;
    private String tableName;

    public TableBean() {
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pName + "\t" + count;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pid);
        dataOutput.writeLong(orderId);
        dataOutput.writeLong(count);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pid = dataInput.readLong();
        this.orderId = dataInput.readLong();
        this.count = dataInput.readLong();
        this.pName = dataInput.readUTF();
        this.tableName = dataInput.readUTF();
    }

    @Override
    public int compareTo(TableBean o) {
        return Long.compare(pid, o.pid);
    }
}
