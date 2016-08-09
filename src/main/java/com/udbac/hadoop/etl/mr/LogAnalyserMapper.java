package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.CombinationKey;
import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.*;
import com.udbac.hadoop.etl.util.LogUtil;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Log的Mapper
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private int inputRecords, filterRecords, outputRecords;
    CombinationKey combinationKey = new CombinationKey();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.debug("Analyse data of :" + value);
        try {
            // 解析日志
            SDCLog sdcLog = LogUtil.handleLog(value.toString());
            if (null==sdcLog) {
                this.filterRecords++;
                return;
            }
            handleData(sdcLog, context);
        } catch (Exception e) {
            this.filterRecords++;
            this.logger.error("处理数据发出异常，数据:" + value, e);
        }
    }

    private void handleData(SDCLog sdcLog, Context context) throws IOException, InterruptedException {
        combinationKey.setFirstKey(sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_DEVICEID));
        combinationKey.setSecondKey(Integer.valueOf(String.valueOf(TimeUtil.timeToLong(sdcLog.getTime()))));
        context.write(combinationKey, new Text(sdcLog.toString()));
    }
}
