package com.ecust.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


/*
客户的那代码套路
1.获取客户端对象
2.执行相关操作命令
3.关闭资源
如：zookeeper、HDFS
* */
public class HDFSCilent {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 1.连接集群的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 2.创建一个配置文件
        Configuration configuration = new Configuration();
        // 设置副本存放数量
        configuration.set("dfs.replication", "2");
        // 用户
        String user = "ls";
        fileSystem = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    // 创建目录
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        /*
        // 1.连接集群的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 2.创建一个配置文件
        Configuration configuration = new Configuration();
        // 用户
        String user = "ls";
        // 3.获取客户端对象
        FileSystem fileSystem = FileSystem.get(uri, configuration, user);
        */

        // 4.创建一个文件夹
        fileSystem.mkdirs(new Path("/xiyou/huaguoshan1"));
        // 5.关闭资源
        fileSystem.close();
    }


    /*
        上传
        参数优先级：
        （1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的自定义配置（xxx-site.xml） >（4）服务器的默认配置（xxx-default.xml）
    * */
    @Test
    public void testUpLoad() throws IOException {
        // 参数1：是否删除原始上传文件       2：是否允许覆盖已存在文件       3.原始数据路径        4.目的路径
        fileSystem.copyFromLocalFile(false, true, new Path("C:\\Users\\creat\\Pictures\\Screenshots\\a.png"), new Path("hdfs://hadoop102/xiyou/huaguoshan/"));

    }

    // 测试文件下载: hadoop fs -get /src /desc
    @Test
    public void testGet() throws IOException {
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fileSystem.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan/a.png"), new Path("D:\\desktop"), false);
    }

    // 测试删除 hadoop fs -rm -r /src
    @Test
    public void testDelete() throws IOException {
        // true表示递归删除
        fileSystem.delete(new Path("/xiyou/huaguoshan1"), true);
    }

    // 测试文件更名或移动： hadoop fs -mv /src /desc
    @Test
    public void testMove() throws IOException {
//        fileSystem.rename(new Path("/xiyou/huaguoshan"), new Path("/xiyou/huaguoshan1"));
        // 文件更名及移动
        fileSystem.rename(new Path("/xiyou/huaguoshan1/a.png"), new Path("/xiyou/b.png"));
    }


    // 获取文件的详细信息
    @Test
    public void testDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus next = files.next();
            System.out.println("==================" + next.getPath() + "==============");
            System.out.println(next.getPermission());
            System.out.println(next.getOwner());
            System.out.println(next.getGroup());
            System.out.println(next.getLen());
            System.out.println(next.getModificationTime());
            System.out.println(next.getReplication());
            System.out.println(next.getBlockSize());
            System.out.println(next.getPath().getName());
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    // 判断文件或文件夹
    @Test
    public void testIsFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        Arrays.stream(fileStatuses).forEach(System.out::println);
    }
    @Test
    public void test1(){
        int n = 198 / 10;
        System.out.println(n);
    }
}
