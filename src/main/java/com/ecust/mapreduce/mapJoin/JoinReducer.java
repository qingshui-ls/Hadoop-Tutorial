package com.ecust.mapreduce.mapJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.util.BeanUtil;

import javax.print.attribute.standard.NumberUp;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<LongWritable, TableBean,TableBean, LongWritable> {
    private final TableBean pdBean = new TableBean();
    private Long index = 0L;
    private final LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(LongWritable key, Iterable<TableBean> values, Reducer<LongWritable, TableBean, TableBean, LongWritable>.Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        for (TableBean value : values) {
            if ("pd".equals(value.getTableName())) {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            } else {
                TableBean tempTableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tempTableBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                orderBeans.add(tempTableBean);
            }
        }
        for (TableBean bean : orderBeans) {
            bean.setpName(pdBean.getpName());
            longWritable.set(index++);
            context.write(bean,longWritable);
        }
    }


}
