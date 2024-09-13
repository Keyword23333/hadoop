package com.test;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class InvertedReducer extends TableReducer<Text,Text, ImmutableBytesWritable> {
    private static final Text result = new Text();
    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        StringBuilder fileList = new StringBuilder();
        for (Text value:values){
            fileList.append(value.toString()).append(";");
        }
        result.set(fileList.toString());
        Put put = new Put(key.toString().getBytes());
        put.addColumn("info".getBytes(), "index".getBytes(), result.toString().getBytes());
        context.write(null, put);
    }
}
