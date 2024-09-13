package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, IOException,InterruptedException {
        Configuration conf = new Configuration();
        //hdfs 主NameNode通信地址
        conf.set("fs.defaultFS","hdfs://hadoop081:9000");
        //yarn 主resourcemanager通信地址
        conf.set("yarn.resourcemanager.hostname","hadoop081");
        //zookeeper集群，连接到HMaster
        conf.set("hbase.zookeeper.quorum","hadoop081,hadoop082,hadoop083");

        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        job.setMapperClass(InvertedMapper.class);
        job.setCombinerClass(InvertedCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //记得更改表名
        TableMapReduceUtil.initTableReducerJob("tabelname",InvertedReducer.class,job);
        System.out.println("Hello world!");
    }
}