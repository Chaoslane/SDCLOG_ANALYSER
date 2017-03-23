package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.RegexFilter;
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

import java.io.IOException;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private LogParserUtil logParserUtil;

    protected void setup(Context context) throws IOException, InterruptedException {
        logParserUtil = new LogParserUtil(context.getConfiguration());
    }

    protected void map(LongWritable key, Text value, Context context) {
        try {
            context.getCounter(LogConstants.MyCounters.ALLLINECOUNTER).increment(1);
            String[] logTokens = StringUtils.split(value.toString(), LogConstants.SEPARTIOR_SPACE);
            //判断日志格式是否正确
            if (StringUtils.isBlank(value.toString()) || logTokens.length != 15) {
                return;
            }
            //传入 -Dfields 参数取指定字段
            String parsedLog = logParserUtil.handleLog(logTokens);
            String parsedQuery = logParserUtil.handleQuery(logTokens);
            context.write(NullWritable.get(), new Text(parsedLog + LogConstants.SEPARTIOR_TAB + parsedQuery));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
//            conf.set("fs.defaultFS", "local");
//            conf.set("fs.defaultFS", "hdfs://192.168.4.3:8022");
        String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (inputArgs.length != 2) {
            System.err.println(LogConstants.INPUTARGSWARN);
            System.exit(-1);
        }
        String inputPath = inputArgs[0];
        String outputPath = inputArgs[1];

        Job job1 = Job.getInstance(conf, "LogAnalyser");
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextInputFormat.setInputPathFilter(job1, RegexFilter.class);
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);
        job1.setJarByClass(LogAnalyserMapper.class);
        job1.setMapperClass(LogAnalyserMapper.class);
        job1.setMapOutputKeyClass(NullWritable.class);
        //无reduce
        job1.setNumReduceTasks(0);
        if (job1.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
            long costTime = (job1.getFinishTime() - job1.getStartTime()) / 1000;
            long linesum = job1.getCounters().findCounter(LogConstants.MyCounters.ALLLINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s");
        } else {
            System.out.println("*****job failed*****");
            System.exit(1);
        }
    }
}

