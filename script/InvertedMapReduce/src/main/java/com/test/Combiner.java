package com.test;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Combiner extends Reducer<Text, Text,Text,Text> {
    private static final Text info = new Text();
    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
        int sum=0;
        for (Text value : values){
            sum+=Integer.parseInt(value.toString());
        }
        int splitIndex = key.toString().indexOf(":");
        info.set(key.toString().substring(0,splitIndex));
        context.write(key,info);
    }
}
