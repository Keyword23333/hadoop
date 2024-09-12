package org.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class invertedindexmapper extends Mapper<LongWritable, Text, Text, Text> 
{
    private Text keyInfo = new Text();    // 表示单词的键
    private Text valueInfo = new Text();  // 表示句子编号和某单词出现次数的
    private Map<String, Map<String, Integer>> wordCounts = new HashMap<>(); // 单词和其在句子中出现的次数

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
        // 将输入的文本行拆分为单词和句子编号
        String[] orderedsentences = value.toString().split(" ");

        // 获取文件名
        FileSplit filesplit = (FileSplit) context.getInputSplit();
        String filename = filesplit.getPath().getName();

        String[] sentences = Arrays.copyOfRange(orderedsentences, 1, orderedsentences.length); // 获取句子中的单词数组

        // 遍历句子中的单词，如果单词不存在则添加
        for (String word : sentences) 
        {
            if (!wordCounts.containsKey(word)) 
            {
                wordCounts.put(word, new HashMap<>());
            }

            // 获取该单词在句子中的出现次数
            Map<String, Integer> sentenceCounts = wordCounts.get(word);
            sentenceCounts.put(filename, sentenceCounts.getOrDefault(filename, 0) + 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException 
    {
        for (Map.Entry<String, Map<String, Integer>> entry : wordCounts.entrySet()) 
        {
            String word = entry.getKey();
            Map<String, Integer> sentenceCounts = entry.getValue();

            // 遍历单词在不同句子中的出现次数
            for (Map.Entry<String, Integer> sentenceEntry : sentenceCounts.entrySet()) 
            {
                keyInfo.set(word); // 键为单词
                valueInfo.set(sentenceEntry.getKey() + ":" + sentenceEntry.getValue()); // 设置值为句子编号:出现次数

                context.write(keyInfo, valueInfo);
            }
        }
    }
}