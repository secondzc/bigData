package com.tongyuan.bigdata.hadoop.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by zhangcy on 2018/11/1
 */
public class WordCount {
    /**
     * 在Mapper父类里接受的内容如下
     * Object：输入数据的具体内容
     * Text：每行的文本数据
     * Text：每个单词分解后的统计结果
     * IntWritable：输出记录的结果
     */
    private static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String lineContent = value.toString(); // 取出每行的数据
            String result[] = lineContent.split(" ");
            for (int i = 0; i < result.length; i++) {
                context.write(new Text(result[i]), new IntWritable(1));
            }
        }
    }

    /**
     * Text: Map输出的文本内容
     * IntWritable：Map处理的个数
     * Text：Reduce输出文本
     * IntWritable：Reduce的输出个数
     */
    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; //保存每个单词出现的次数
            for (IntWritable count : values) {
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.out.println("本程序需要两个参数，执行：hadoop jar wordCount.jar /input/info.txt /output");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        // 考虑到最终需要使用HDFS进行内容的处理操作，并购且输入的时候不带有HDFS地址
        String[] argArray = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "hadoop");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(WordCountReducer.class);
        // 随后需要设置Map-Reduce最终的执行结果
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入及输出路径
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        // 等待执行完毕
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
