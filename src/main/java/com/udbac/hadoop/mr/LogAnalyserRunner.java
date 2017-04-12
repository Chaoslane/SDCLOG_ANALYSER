package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.RegexFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by root on 2017/3/23.
 */
public class LogAnalyserRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LogAnalyserRunner(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
//        configuration.set("fs.defaultFS", "hdfs://192.168.4.3:8022");
        conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
        if (args.length != 2
                || StringUtils.isBlank(conf.get("filename.pattern"))
                || StringUtils.isBlank(conf.get("fields.column"))) {
            System.err.println(LogConstants.INPUTARGSWARN);
            System.exit(-1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

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
        }
        return job1.waitForCompletion(true) ? 0 : 1;
    }

}
