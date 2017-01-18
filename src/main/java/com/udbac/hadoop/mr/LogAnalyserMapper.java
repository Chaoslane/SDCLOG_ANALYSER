package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
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
    private final static Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private Map<NullWritable,Text> mapoutput = new HashMap<>();
    private String fields = null;

    protected void setup(Context context){
        Configuration configuration = context.getConfiguration();
        fields = configuration.get("com.udbac.fields");
        mapoutput.clear();
    }

    protected void map(LongWritable key, Text value, Context context){
        try {
            String[] logTokens = StringUtils.split(value.toString(), LogConstants.LOG_SEPARTIOR);
            if (!StringUtils.isNotBlank(fields)) {
                throw new RuntimeException("fields is null");
            }
            if (logTokens.length == 15){
                String sdcLog = LogParserUtil.handleLog(logTokens, fields.split(","));
                context.write(NullWritable.get(), new Text(sdcLog));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

}
