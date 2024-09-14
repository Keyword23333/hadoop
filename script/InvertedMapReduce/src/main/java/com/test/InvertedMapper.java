package com.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;


public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private final Text keyInfo = new Text();    // 表示单词的键
    private final Text valueInfo = new Text("1");  // 表示句子编号和某单词出现次数的// 单词和其在句子中出现的次数

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 将输入的文本行拆分为单词和句子编号
        String[] orderedSentences = value.toString().split(" ");

        // 获取文件名
        FileSplit filesplit = (FileSplit) context.getInputSplit();
        String filename = filesplit.getPath().getName();

        String[] sentences = Arrays.copyOfRange(orderedSentences, 1, orderedSentences.length); // 获取句子中的单词数组

        // 遍历句子中的单词，构建mapper，输出的形式应该是<单词:文件名,1>
        for (String word : sentences)
        {
            // 获取该单词在句子中的出现次数
            keyInfo.set(word+":"+filename);
            context.write(keyInfo,valueInfo);
        }
    }
}
