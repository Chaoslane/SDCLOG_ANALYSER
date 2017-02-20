package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.example.MyApp;

import java.io.IOException;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static String[] fields ;

    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration configuration = context.getConfiguration();
        fields = StringUtils.split(configuration.get("fields").trim(),LogConstants.SEPARTIOR_COMMA);
        if (fields.length == 0) {
            throw new RuntimeException("fields is null");
        }
    }

    protected void map(LongWritable key, Text value, Context context) {
        try {
            String[] logTokens = StringUtils.split(value.toString(), LogConstants.SEPARTIOR_SPACE);
            //判断日志格式是否正确
            if (StringUtils.isBlank(value.toString()) || logTokens.length != 15) {
                return;
            }
            //传入 -Dfields 参数取指定字段
            String sdcLog = LogParserUtil.handleLog(logTokens, fields);
            if (StringUtils.isNotBlank(sdcLog)) {
                context.getCounter(LogConstants.MyCounters.ALLLINECOUNTER).increment(1);
                context.write(NullWritable.get(), new Text(sdcLog));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "local");
//            conf.set("fs.defaultFS", "hdfs://192.168.4.2:8022");
            String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (inputArgs.length != 2) {
                System.err.println("\"Usage:<-Dfields=[field1?field2]> <inputPath> <outputPath>/n\"");
                System.exit(2);
            }
            String inputPath = inputArgs[0];
            String outputPath = inputArgs[1];

            Job job1 = Job.getInstance(conf, "LogAnalyser");
            TextInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));
            LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
            TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);
            job1.setJarByClass(LogAnalyserMapper.class);
            job1.setMapperClass(LogAnalyserMapper.class);
            job1.setMapOutputKeyClass(NullWritable.class);
            if (job1.waitForCompletion(true)) {
                System.out.println("-----job succeed-----");
                System.out.println("-----alllines count-----:" +
                        job1.getCounters().findCounter(LogConstants.MyCounters.ALLLINECOUNTER).getValue());
            } else {
                System.exit(1);
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("*****job failed*****");
        }
    }
}
