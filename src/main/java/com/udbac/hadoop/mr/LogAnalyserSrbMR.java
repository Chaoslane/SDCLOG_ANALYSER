package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.Constants;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.common.PairWritable;
import com.udbac.hadoop.util.IPParser;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 2017/6/6.
 */
public class LogAnalyserSrbMR {

    static class SessionMapper extends Mapper<LongWritable, Text, PairWritable, Text> {
        private static Logger logger = Logger.getLogger(SessionMapper.class);
        private static IPParser ipParser = new IPParser.Cache();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(Constants.MyCounters.LINECOUNTER).increment(1);
            try {
                Map<String, String> logMap = LogParser.logParserSDC(value.toString());

                logMap.put("prov", ipParser.getArea(logMap.get("c_ip"))[0]);
                logMap.put("city", ipParser.getArea(logMap.get("c_ip"))[1]);

                String ckid = StringUtils.defaultIfEmpty(logMap.get("ckid"), "");
                String date_time = StringUtils.defaultIfEmpty(logMap.get("date_time"), "");
                logMap.remove("ckid");
                logMap.remove("date_time");

                SplitValueBuilder svb = new SplitValueBuilder("\t").add(date_time);
                for (String field : LogAnalyserRunner.getFieldsColum().split(",")) {
                    svb.add(StringUtils.defaultIfBlank(logMap.get(field), ""));
                }

                context.write(new PairWritable(ckid, date_time), new Text(svb.toString()));
            } catch (LogParseException e) {
                context.getCounter(Constants.MyCounters.FAILEDMAPPERLINE).increment(1);
                logger.error(e.getMessage());
            }
        }
    }

    static class SessionReducer extends Reducer<PairWritable, Text, NullWritable, Text> {
        private static Logger logger = Logger.getLogger(SessionReducer.class);

        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                String cookieId = key.getCookieId();
                List<Long> timeList = new ArrayList<>();
                for (Text v : values) {
                    String date_time = v.toString().split("\t",2)[0];
                    String value = v.toString().split("\t", 2)[1];
                    Long currTime = TimeUtil.parseString2Long(date_time) / 1000;

                    //两条日志间隔时间计算
                    Long timeInterval = 0L;
                    if (timeList.size() > 0) {
                        timeInterval = currTime - timeList.get(timeList.size() - 1);
                        if ((currTime - timeList.get(0)) > Constants.HALFHOUR_OF_SECONDS) {
                            timeList.clear();
                        }
                    }

                    timeList.add(currTime);
                    String ssid = cookieId + ":" + timeList.get(0);
                    Text text = new Text(
                            date_time + "\t" + cookieId + "\t" + ssid + "\t" + timeInterval + "\t" + value);
                    context.write(NullWritable.get(), text);
                }
            } catch (LogParseException e) {
                context.getCounter(Constants.MyCounters.FAILEDREDUCERLINE).increment(1);
                logger.error(e.getMessage());
            }
        }
    }

}
