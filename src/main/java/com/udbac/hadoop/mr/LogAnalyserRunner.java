package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.Constants;
import com.udbac.hadoop.common.PairWritable;
import com.udbac.hadoop.common.RegexFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by root on 2017/6/8.
 */
public class LogAnalyserRunner extends Configured implements Tool {

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
//        conf.set("fs.defaultFS", "hdfs://192.168.4.3:8022");
//        conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
    }

    @Override
    public int run(String[] args) throws Exception {
        initArgs();
        if (args.length != 2
                || StringUtils.isBlank(filenamePattern)
                || StringUtils.isBlank(fieldsColum)
                || StringUtils.isBlank(logDate)) {
            System.err.println(Constants.INPUTARGSWARN);
            System.exit(-1);
        }

        Job job;
        if (isSSbuild) {
            job = Job.getInstance(getConf(), "wt-session-build");
            job.setJarByClass(LogAnalyserSrbMR.class);
            job.setMapperClass(LogAnalyserSrbMR.SessionMapper.class);
            job.setMapOutputKeyClass(PairWritable.class);
            job.setReducerClass(LogAnalyserSrbMR.SessionReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setPartitionerClass(PairWritable.PairPartitioner.class);
            job.setSortComparatorClass(PairWritable.PairComparator.class);
            job.setGroupingComparatorClass(PairWritable.PairGrouping.class);
        } else {
            job = Job.getInstance(getConf(), "wt-log-analyser");
            job.setJarByClass(LogAnalyserMapper.class);
            job.setJarByClass(LogAnalyserMapper.class);
            job.setMapperClass(LogAnalyserMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setNumReduceTasks(0);
        }

        //input & output
        String inputPath = args[0];
        String outputPath = args[1];

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputPathFilter(job, RegexFilter.class);
        FileInputFormat.setInputDirRecursive(job, true);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
            long costTime = (job.getFinishTime() - job.getStartTime()) / 1000;
            long linesum = job.getCounters().findCounter(Constants.MyCounters.LINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s ");
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }


    private static String  logDate;
    private static String  filenamePattern;
    private static String  fieldsColum;
    private static Boolean isSSbuild;

    private void initArgs() {
        logDate = getConf().get("log.date");
        filenamePattern = getConf().get("filename.pattern");
        fieldsColum = getConf().get("fields.column");
        isSSbuild = Boolean.valueOf(getConf().get("ss.build"));
    }

    public static String getLogDate() {
        return logDate;
    }

    public static String getFieldsColum() {
        return fieldsColum;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LogAnalyserRunner(), args);
        System.exit(res);
    }

}
