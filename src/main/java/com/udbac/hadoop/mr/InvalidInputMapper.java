package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.Constants;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.common.RegexFilter;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by root on 2017/6/20.
 */
public class InvalidInputMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    String date;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        date = context.getConfiguration().get("log.date");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        if (tokens.length != 17) {
            context.getCounter(Constants.MyCounters.LINECOUNTER).increment(1);
            context.write(NullWritable.get(), new Text("format error: " + value.toString()));
        }
        if (tokens.length == 17) {
            try {
                String dateTime = TimeUtil.handleTime(tokens[2] + " " + tokens[3]);
                if (!dateTime.contains(date)) {
                    context.getCounter(Constants.MyCounters.LINECOUNTER).increment(1);
                    context.write(NullWritable.get(),new Text("date error: " +value.toString()));
                }
            } catch (LogParseException e) {
                context.getCounter(Constants.MyCounters.LINECOUNTER).increment(1);
                context.write(NullWritable.get(), new Text("time error: " + value.toString()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] reargs = optionsParser.getRemainingArgs();
        String inputPath = reargs[0];
        String outputPath = reargs[1];

        Job job = Job.getInstance(conf, "invalid-input");
        job.setJarByClass(InvalidInputMapper.class);
        job.setMapperClass(InvalidInputMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);

        //input & output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputPathFilter(job, RegexFilter.class);
        FileInputFormat.setInputDirRecursive(job, true);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.setNumReduceTasks(0);

        if (job.waitForCompletion(true)) {
            System.out.println("job succeed" + job.getCounters().findCounter(Constants.MyCounters.LINECOUNTER).getValue());
        }
    }
}
