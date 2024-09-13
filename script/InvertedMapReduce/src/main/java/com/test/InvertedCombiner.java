package com.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedCombiner extends Reducer<Text,Text,Text,Text> {
    private final Text valueInfo = new Text();

    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        int sum = 0;
        for (Text value:values){
            sum += Integer.parseInt(value.toString());
        }
        int fileNameIndex = key.toString().indexOf(":");
        //重设value和key值
        valueInfo.set(key.toString().substring(fileNameIndex+1)+":"+sum);
        key.set(key.toString().substring(0,fileNameIndex));
        //输出应该是<单词,文件名:次数>
        context.write(key,valueInfo);
    }
}
