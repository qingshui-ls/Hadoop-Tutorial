package com.ecust.mapreduce.cacheJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JoinMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {
    private final TableBean tableBean = new TableBean();
    private Map<Long, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {

        //通过缓存文件得到小表数据 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        //通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {            //切割一行
            //01 小米
            String[] sp = line.split("\t");
            pdMap.put(Long.parseLong(sp[0]), sp[1]);
        }
        //关流
        IOUtils.closeStream(reader);

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        tableBean.setPid(Long.parseLong(split[1]));
        tableBean.setCount(Long.parseLong(split[2]));
        tableBean.setOrderId(Long.parseLong(split[0]));
        tableBean.setpName(pdMap.get(tableBean.getPid()));
        tableBean.setTableName("order");

        context.write(tableBean, NullWritable.get());
    }
}
