package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.CombinationKey;
import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.AnalysedLog;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by root on 2016/7/26.
 */
public class EndAnalyseMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private String lastdeviceId;
    private long last = 0;
    private long cur = 0;
    private long tmp = 0;
    private BigDecimal duration;
    private Map<String, String> event = new HashMap<String, String>();
    private Map<String, AnalysedLog> oneVisit = new HashMap<String, AnalysedLog>();
    private AnalysedLog analysedLog = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        event.clear();
        oneVisit.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int index = value.toString().indexOf("\t");
        String log = value.toString().substring(index + 1);
        String[] logSplit = log.split("\\|");

        if (10 == logSplit.length) {
            String[] eventSplit = logSplit[9].split(":");
            event.put(eventSplit[0], eventSplit[1]);
        }

        cur = TimeUtil.timeToLong(logSplit[1]);
        if (logSplit[0].equals(lastdeviceId) && cur - last < SDCLogConstants.HALFHOUR_OF_MILLISECONDS) {
            tmp = cur - last;
            duration = duration.add(BigDecimal.valueOf(tmp));
            analysedLog.setDuration(duration);
        } else {
            duration = BigDecimal.ZERO;
            analysedLog = handleLog(logSplit, event, duration);
        }
        lastdeviceId = logSplit[0];
        last = cur;
        oneVisit.put(analysedLog.getVisitId(), analysedLog);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (AnalysedLog analysedLog : oneVisit.values()) {
            context.write(NullWritable.get(), new Text(analysedLog.toString()));
        }
        super.cleanup(context);
    }

    private static AnalysedLog handleLog(String[] logSplit, Map event, BigDecimal duration) {
        AnalysedLog analysedLog = new AnalysedLog();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        analysedLog.setVisitId(uuid);
        analysedLog.setDeviceId(logSplit[0]);
        analysedLog.setTime(logSplit[1]);
        analysedLog.setDate(logSplit[2]);
        analysedLog.setcIp(logSplit[3]);
        analysedLog.setCsUserAgent(logSplit[4]);
        analysedLog.setUtmSource(logSplit[5]);
        analysedLog.setuType(logSplit[6]);
        analysedLog.setWtAvv(logSplit[7]);
        analysedLog.setWtPos(logSplit[8]);
        analysedLog.setEveMap(event);
        analysedLog.setDuration(duration);
        analysedLog.setPageView("1");
        return analysedLog;
    }

//    public static void main(String[] args) {
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop-01:8020");
////        conf.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
//        try {
//            Job job = Job.getInstance(conf, "LogAnalyser");
//            FileSystem fs = FileSystem.get(conf);
//
//            job.setJarByClass(EndAnalyseMapper.class);
//
//            ChainMapper.addMapper(job, EndAnalyseMapper.class, LongWritable.class, Text.class, NullWritauteble.class, Text.class, conf);
//
//            FileInputFormat.addInputPath(job, new Path("/user/mr/2016-07-25/output"));
//            //output目录不允许存在。
//            Path output = new Path("/user/mr/2016-07-25/output/out");
//            if (fs.exists(output)) {
//                fs.delete(output, true);
//            }
//            FileOutputFormat.setOutputPath(job, output);
//
//            boolean f = job.waitForCompletion(true);
//            if (f) {
//                System.out.println("job 执行成功");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
