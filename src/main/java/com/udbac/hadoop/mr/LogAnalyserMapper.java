package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(LogConstants.MyCounters.ALLLINECOUNTER).increment(1);
        Text text = null;
        try {
            text = new Text(LogParser.handleLog(value.toString()));
        } catch (UnsupportedEncodingException e) {
            logger.info(e.getMessage());
        }
        context.write(NullWritable.get(), text);
    }
}

