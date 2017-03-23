package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private LogParserParser logParserParser;

    protected void setup(Context context) throws IOException, InterruptedException {
        logParserParser = new LogParserParser(context.getConfiguration());
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
            String parsedLog = logParserParser.handleLog(logTokens);
            context.write(NullWritable.get(), new Text(parsedLog));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

