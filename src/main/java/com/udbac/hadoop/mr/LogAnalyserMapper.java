package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Logger logger = Logger.getLogger(LogAnalyserMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        final Map<String, String> stringStringMap;
        try {
             stringStringMap = LogParser.logParserSDC(line);
        } catch (LogParseException e) {
            logger.warn(e.getMessage());
        }
    }
}

