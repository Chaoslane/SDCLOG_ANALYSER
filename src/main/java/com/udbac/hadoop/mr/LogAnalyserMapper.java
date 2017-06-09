package com.udbac.hadoop.mr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.util.SplitValueBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
    private static String[] fieldsColumn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.getCounter(LogAnalyserRunner.MyCounters.ALLLINECOUNTER).increment(1);
        Configuration conf = context.getConfiguration();
        fieldsColumn = conf.get("fields.column").split(",");
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Map<String, String> logMap = LogParser.logParserSDC(value.toString());
            String res = LogParser.getResStr(logMap, fieldsColumn);
            if (StringUtils.isNotBlank(res))
                context.write(NullWritable.get(), new Text(res));
        } catch (LogParseException e) {
            logger.error(e.getMessage());
        }
    }
}

