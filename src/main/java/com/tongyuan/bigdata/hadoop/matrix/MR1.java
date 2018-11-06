package com.tongyuan.bigdata.hadoop.matrix;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by zhangcy on 2018/11/6
 * 矩阵相乘，matrix1*matrix2
 * 思路是将右侧矩阵转置，然后与左矩阵按行相乘
 * 分成两步，两个map-reduce作业
 *
 * 文件的格式是：1 1_0,2_3,3_1,4_2
 *             2 1_2,2_4,3_1,4_6
 */
public class MR1 {
    //输入文件相对路径
    private static String inPath = "/matrix/step1_input/matrix2.txt";
    //输出文件相对路径
    private static String outPahth = "/matrix/step1_output";

    private static String hdfs = "hdfs://master:9000";
    public int run(){
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfs);
            Job job = Job.getInstance(conf, "step1");

            job.setJarByClass(MR1.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            if(fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }

            Path outputPath = new Path(outPahth);
            fs.delete(outputPath, true);
            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new MR1().run();
        if(result == 1) {
            System.out.println("step1 success");
        } else if (result == -1) {
            System.out.println("step1 fail");
        }
    }
}
