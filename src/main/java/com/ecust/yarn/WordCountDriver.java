package com.ecust.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class WordCountDriver {
    private static Tool tool;

    public static void main(String[] args) throws Exception {
        // 创建配置
        Configuration configuration = new Configuration();
        switch (args[0]) {
            case "wordcount":
                tool = new WordCount();
                break;
            default:
                throw new RuntimeException("no such tool" + args[0]);
        }

        // 执行
        int run = ToolRunner.run(configuration, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(run);
    }
}
