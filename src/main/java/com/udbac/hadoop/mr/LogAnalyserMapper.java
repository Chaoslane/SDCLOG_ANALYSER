package com.udbac.hadoop.mr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.common.LogParser;
import com.udbac.hadoop.util.SplitValueBuilder;
import org.apache.commons.lang.StringUtils;
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
    private static Gson gson = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fieldsColumn = context.getConfiguration().get("fields.column").split(",");
        gson = new GsonBuilder().disableHtmlEscaping().create();
        // 获取输入日期参数
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(LogConstants.MyCounters.LINECOUNTER).increment(1);
        try {
            Map<String, String> logMap = LogParser.logParserSDC(value.toString());

            SplitValueBuilder svb = new SplitValueBuilder("\t");
            for (String field : fieldsColumn) {
                svb.add(StringUtils.defaultIfEmpty(logMap.get(field),""));
                logMap.remove(field);
            }

            String res = svb.add(gson.toJson(logMap)).build();
            context.write(NullWritable.get(), new Text(res));
        } catch (LogParseException e) {
            logger.error(e.getMessage());
        }
    }

}

