package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.common.PairWritable;
import com.udbac.hadoop.util.IPCacheParser;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 2017/6/6.
 */
public class SessionRebuild {

    static class SessionMapper extends Mapper<LongWritable, Text, PairWritable, Text> {
        private static Logger logger = Logger.getLogger(SessionMapper.class);
        private static IPCacheParser ipParser = null;
        private static String[] fieldsColumn = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            ipParser = IPCacheParser.getSingleIPParser();
            fieldsColumn = context.getConfiguration().get("fields.column").split(",");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(LogConstants.MyCounters.LINECOUNTER).increment(1);
            try {
                Map<String, String> logMap = LogParser.logParserSDC(value.toString());
                logMap.put("prov", ipParser.getArea(logMap.get("c_ip")).split(",")[0]);
                logMap.put("city", ipParser.getArea(logMap.get("c_ip")).split(",")[1]);

                String ckid = StringUtils.defaultIfEmpty(logMap.get("ckid"), "");
                String date_time = StringUtils.defaultIfEmpty(logMap.get("date_time"), "");

                logMap.remove("ckid");
                logMap.remove("date_time");

                SplitValueBuilder svb = new SplitValueBuilder("\t");
                String restoken = null;
                for (String field : fieldsColumn) {
                    restoken = StringUtils.defaultIfEmpty(logMap.get(field), "");
                    svb.add(restoken);
                }

                context.write(new PairWritable(ckid, date_time), new Text(svb.build()));
            } catch (LogParseException e) {
                context.getCounter(LogConstants.MyCounters.FAILEDMAPPERLINE).increment(1);
                logger.error(e.getMessage());
            }
        }
    }

    static class SessionReducer extends Reducer<PairWritable, Text, NullWritable, Text> {
        private static Logger logger = Logger.getLogger(SessionReducer.class);
        Map<String, List<Long>> cookieTime = new HashMap<>();

        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                Long currTime = TimeUtil.parseStringDate2Long(key.getDateTime()) / 1000;

                List<Long> timeList = cookieTime.get(key.getCookieId());
                if (null == timeList) {
                    timeList = new ArrayList<>();
                } else if ((currTime - cookieTime.get(key.getCookieId()).get(0)) > LogConstants.HALFHOUR_OF_SECONDS) {
                    timeList.clear();
                }

                for (Text v : values) {
                    timeList.add(currTime);
                    cookieTime.put(key.getCookieId(), timeList);
                    String ssid = key.getCookieId() + ":" + timeList.get(0);
                    //两条日志间隔时间计算
                    Long timeInterval = 0L;
                    if (timeList.size() > 1)
                        timeInterval = timeList.get(timeList.size() - 1) - timeList.get(timeList.size() - 2);
                    Text text = new Text(
                            key.getDateTime() + "\t"+key.getCookieId() + "\t" + ssid + "\t" + timeInterval + "\t" + v.toString());
                    context.write(NullWritable.get(), text);
                }
            } catch (LogParseException e) {
                context.getCounter(LogConstants.MyCounters.FAILEDREDUCERLINE).increment(1);
                System.out.println(e.getMessage());
                logger.error(e.getMessage());
            }
        }
    }

}
