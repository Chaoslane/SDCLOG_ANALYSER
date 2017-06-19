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

    private boolean isParamsBlank(Configuration conf) {
        return StringUtils.isBlank(conf.get("filename.pattern"))
                || StringUtils.isBlank(conf.get("fields.column"));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2 || isParamsBlank(getConf())) {
            System.err.println(LogConstants.INPUTARGSWARN);
            System.exit(-1);
        }

        Configuration conf = getConf();
//        conf.set("fs.defaultFS", "hdfs://192.168.4.3:8022");
//        conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance(conf, "sdclog-analyser");
        job.setJarByClass(LogAnalyserMapper.class);
        job.setMapperClass(LogAnalyserMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);

        //input & output
        TextInputFormat.addInputPath(job, new Path(inputPath));
        TextInputFormat.setInputPathFilter(job, RegexFilter.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        if (job.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
            long costTime = (job.getFinishTime() - job.getStartTime()) / 1000;
            long linesum = job.getCounters().findCounter(LogConstants.MyCounters.LINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s");
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }


}
