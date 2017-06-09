package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.common.PairWritable;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
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
public class SessionRebuild{

    static class SessionMapper extends Mapper<LongWritable, Text, PairWritable, Text>{
        private static String[] fieldsColumn = null;
        private static Logger logger = Logger.getLogger(SessionMapper.class);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.getCounter(LogAnalyserRunner.MyCounters.ALLLINECOUNTER).increment(1);
            Configuration conf = context.getConfiguration();
            fieldsColumn = conf.get("fields.column").split(",");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Map<String, String> logMap = LogParser.logParserSDC(value.toString());
                String ckid = null == logMap.get("ckid") ? "" : logMap.get("ckid");
                String date_time = null == logMap.get("date_time") ? "" : logMap.get("date_time");
                String res = LogParser.getResStr(logMap, fieldsColumn);
                context.write(
                        new PairWritable(ckid, date_time), new Text(res));
            } catch (LogParseException e) {
                logger.error(e.getMessage());
            }
        }
    }

    static class SessionReducer extends Reducer<PairWritable, Text, NullWritable, Text> {
        Map<String, List<Long>> cookieTime = new HashMap<>();

        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                Long currTime = TimeUtil.parseStringDate2Long(key.getDateTime()) / 1000;

                List<Long> timeList = cookieTime.get(key.getCookieId());
                if (null == timeList) {
                    timeList = new ArrayList<>();
                }else if ((currTime - cookieTime.get(key.getCookieId()).get(0)) > LogConstants.HALFHOUR_OF_SECONDS) {
                    timeList.clear();
                }

                for (Text v : values) {
                    timeList.add(currTime);
                    cookieTime.put(key.getCookieId(),timeList);
                    String ssid = key.getCookieId() + ":" + timeList.get(0);
                    //两条日志间隔时间
                    String timeInterval = String.valueOf(0);
                    if (timeList.size() > 1)
                        timeInterval = String.valueOf(
                                timeList.get(timeList.size()-1) - timeList.get(timeList.size()-2));
                    context.write(NullWritable.get(),new Text(
                            key.getCookieId()+"\t"+ssid+"\t"+key.getDateTime()+"\t"+timeInterval));
                }
            } catch (LogParseException e) {
                e.printStackTrace();
            }
        }
    }

}
