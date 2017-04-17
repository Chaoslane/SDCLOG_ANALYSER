package com.udbac.hadoop.mr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
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
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private static String[] fieldsColumn = null;
    private static String[] fieldsJson = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        if (StringUtils.isNotBlank(conf.get("fields.column")))
            fieldsColumn = conf.get("fields.column").split(",");
        if (StringUtils.isNotBlank(conf.get("fields.json")))
            fieldsJson = conf.get("fields.json").split(",");
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Map<String, String> logMap = LogParser.logParserSDC(value.toString());
            String res = getResStr(logMap);
            if (StringUtils.isNotBlank(res))
                context.write(NullWritable.get(), new Text(res));
        } catch (LogParseException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * 取程序输入参数 从logmap中取的参数字段值 fieldsJson为空 则取fieldsColumn差集
     * @param logMap 一条日志的所有字段值
     * @return 拼接结果字符串
     */
    private static String getResStr(Map<String, String> logMap) {
        SplitValueBuilder svb = new SplitValueBuilder("\t");
        if (null != fieldsColumn) {
            for (String field : fieldsColumn) {
                String[] multi = StringUtils.split(field, "?");
                String value = "";
                for (String k : multi) {
                    if (StringUtils.isNotBlank(logMap.get(k))) {
                        value = logMap.get(k);
                        break;
                    }
                }
                svb.add(value);
                for (String k : multi) {
                    logMap.remove(k);
                }
            }
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        if (null == fieldsJson) {
            svb.add(gson.toJson(logMap));
        } else {
            for (String field : fieldsJson) {
                Map<String, String> selectJson = new HashMap<>();
                String[] multi = StringUtils.split(field, "?");
                for (String k : multi) {
                    if (StringUtils.isNotBlank(logMap.get(k))) {
                        selectJson.put(k, logMap.get(k));
                        break;
                    }
                }
                svb.add(gson.toJson(selectJson));
            }
        }
        return svb.toString();
    }
}

