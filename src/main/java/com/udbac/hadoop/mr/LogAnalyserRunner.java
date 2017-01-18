package com.udbac.hadoop.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;


/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class LogAnalyserRunner {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.4.2:8022");
//            conf.addResource(LogAnalyserRunner.class.getResourceAsStream("/conf.xml"));

            String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (inputArgs.length != 2) {
                System.err.println("\"Usage:<inputPath><outputPath>/n\"");
                System.exit(2);
            }
            String inputPath = inputArgs[0];
            String outputPath = inputArgs[1];
//        String ipPath = inputArgs[2];

            Job job1 = Job.getInstance(conf, "LogAnalyser");
            TextInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));
            job1.setJarByClass(LogAnalyserRunner.class);
            job1.setMapperClass(LogAnalyserMapper.class);

            job1.setMapOutputKeyClass(NullWritable.class);
//        job1.addCacheFile(new URI(ipPath + "/udbacIPtransArea.csv"));
//        job1.addCacheFile(new URI(ipPath + "/udbacIPtransSegs.csv"));

            if(job1.waitForCompletion(true)){
                System.out.println("job执行成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
