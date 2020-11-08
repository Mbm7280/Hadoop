package com.hadp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


/*
 * @Author maiBangMin
 * @Description [HDFSApi-Client]
 * @Date 4:30 下午 2020/11/8
 * @Version 1.0
 **/
public class HDFSClient {

    FileSystem fileSystem;

    @Test
    public void put() throws IOException, InterruptedException {
        // 2. 操作集群
        // 2.1 上传
        fileSystem.copyFromLocalFile(new Path("/Users/maibangmin/app/code/1.txt"),new Path("/"));
    }

    @Test
    public void get() throws IOException {
        // 2. 操作集群
        // 2.2 下载
        fileSystem.copyToLocalFile(new Path("/LICENSE.txt"),new Path("/Users/maibangmin/app/vue"));
    }

    @Test
    public void append() throws IOException {
        // 2. 操作集群
        // 2.3 Hdfs数据追加
        FSDataOutputStream append = fileSystem.append(new Path("/LICENSE.txt"));
        append.write("TetsApi".getBytes());
        IOUtils.closeStream(append);

    }

    @Test
    public void ls() throws IOException {
        // 2. 操作集群
        // 2.4 ls查看文件
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus f:fileStatuses) {
            System.out.println("path：" + f.getPath());
            System.out.println("owner：" + f.getOwner());
            System.out.println("BlockSize：" + f.getBlockSize());
        }
    }






    @Before
    public void before() throws IOException, InterruptedException {
        // 可配置Configuration
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        configuration.set("dfs.blocksize","67188864");

        // 1. 新建HDFS对象
        fileSystem = FileSystem.get(URI.create("hdfs://192.168.143.161:8020"),
                new Configuration(), "hadp"); // Configuration 对应Hadoop中 core.site.xml 中的<Configuration>标签
    }

    @After
    public void after() throws IOException {
        // 3. 关闭资源
        fileSystem.close();
    }

}
