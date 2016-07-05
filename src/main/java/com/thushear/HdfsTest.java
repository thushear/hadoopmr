package com.thushear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by kongming on 2016/7/5.
 */
public class HdfsTest {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://master:9000";
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),configuration);

        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }

        //hadoop 权限设置同于linux权限
        FSDataOutputStream os = fs.create(new Path("/user/hadoop/test.log"));
        os.write("hello hadoop!".getBytes());
        os.flush();
        os.close();

        InputStream is = fs.open(new Path("/user/hadoop/test.log"));
        IOUtils.copyBytes(is,System.out,1024,true);

    }

}
