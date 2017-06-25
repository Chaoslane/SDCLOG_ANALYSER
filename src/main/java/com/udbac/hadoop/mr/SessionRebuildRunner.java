package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
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
public class SessionRebuildRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SessionRebuildRunner(), args);
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
        job.setJarByClass(SessionRebuild.class);

        job.setMapperClass(SessionRebuild.SessionMapper.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setReducerClass(SessionRebuild.SessionReducer.class);
        job.setOutputKeyClass(NullWritable.class);

        job.setPartitionerClass(PairWritable.PairPartitioner.class);
        job.setSortComparatorClass(PairWritable.PairComparator.class);
        job.setGroupingComparatorClass(PairWritable.PairGrouping.class);

        //input & output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputPathFilter(job, RegexFilter.class);
        FileInputFormat.setInputDirRecursive(job, true);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
            long costTime = (job.getFinishTime() - job.getStartTime()) / 1000;
            long linesum = job.getCounters().findCounter(LogConstants.MyCounters.LINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s ");
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }


}
