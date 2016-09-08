package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.AnalysedLog;
import com.udbac.hadoop.etl.util.IPSeekerExt;
import com.udbac.hadoop.util.TimeUtil;
import com.udbac.hadoop.util.UserAgentUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by root on 2016/7/26.
 */
public class SessionBuildMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();
    File file = null;
    private String lastdeviceId;
    private long last = 0;
    private long cur = 0;
    private long tmp = 0;
    private BigDecimal duration = null;
    private AnalysedLog analysedLog = null;
    private Map<String, AnalysedLog> oneVisit = new HashMap<String, AnalysedLog>();

    private static AnalysedLog handleLog(String[] logSplit, BigDecimal duration) {
        AnalysedLog analysedLog = new AnalysedLog();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        analysedLog.setVisitId(uuid);
        analysedLog.setDeviceId(logSplit[0]);
        analysedLog.setTime(logSplit[1]);
        analysedLog.setDate(logSplit[2]);
        analysedLog.setcIp(logSplit[3]);
//        handleIP(analysedLog);
        analysedLog.setCsUserAgent("Agent");
//        handleUserAgent(analysedLog);
        analysedLog.setUtmSource(logSplit[5]);
        analysedLog.setuType(logSplit[6]);
        analysedLog.setWtAvv(logSplit[7]);
        analysedLog.setWtPos(logSplit[8]);
        analysedLog.setDuration(duration);
        analysedLog.setPageView("1");
        return analysedLog;
    }

    private static void handleIP(AnalysedLog analysedLog) {
        String uip = analysedLog.getcIp();
        if (StringUtils.isNotBlank(uip)) {
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(uip);
            if (info != null) {
                analysedLog.setcIp(info.getCountry() + "," + info.getProvince() + "," + info.getCity());
            }
        }
    }

    private static void handleUserAgent(AnalysedLog analysedLog) {
        String csUserAgent = analysedLog.getCsUserAgent();
        if (StringUtils.isNotBlank(csUserAgent)) {
            UserAgentUtil.UserAgentInfo info = UserAgentUtil.analyticUserAgent(csUserAgent);
            if (info != null) {
                analysedLog.setCsUserAgent(info.toString());
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
//            URI mappingFileUri = context.getCacheFiles()[0];
//            if (mappingFileUri != null) {
//                // Would probably be a good idea to inspect the URI to see what the bit after the # is, as that's the file name
//                System.out.println("Mapping File: " + FileUtils.readFileToString(new File("./qqwry.dat")));
//            } else {
//                System.out.println(">>>>>> NO MAPPING FILE");
//            }
//        } else {
//            System.out.println(">>>>>> NO CACHE FILES AT ALL");
//        }
        oneVisit.clear();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int index = value.toString().indexOf("\t");
        String log = value.toString().substring(index + 1);
        String[] logSplits = log.split("\\|");
        String[] eventSplits = null;

        if (10 == logSplits.length) {
            eventSplits = logSplits[9].split(":");
        }
        cur = TimeUtil.timeToLong(logSplits[1]);
        if (logSplits[0].equals(lastdeviceId) && cur - last < SDCLogConstants.HALFHOUR_OF_MILLISECONDS) {
            tmp = cur - last;
            duration = duration.add(BigDecimal.valueOf(tmp));
            analysedLog.setDuration(duration);
            if (null != eventSplits) {
                analysedLog.getEveMap().put(eventSplits[0], eventSplits[1]);
            }
        } else {
            duration = BigDecimal.ZERO;
            analysedLog = handleLog(logSplits, duration);
            if (null != eventSplits) {
                analysedLog.getEveMap().put(eventSplits[0], eventSplits[1]);
            }
        }
        lastdeviceId = logSplits[0];
        last = cur;
        oneVisit.put(analysedLog.getVisitId(), analysedLog);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (AnalysedLog analysedLog1 : oneVisit.values()) {
            context.write(NullWritable.get(), new Text(analysedLog1.toString()));
        }
        super.cleanup(context);
    }

//    public static void main(String[] args) {
//        Configuration conf = new Configuration();
////        conf.set("fs.defaultFS", "hdfs://hadoop-01:8020");
////        conf.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
//        try {
//            Job job = Job.getInstance(conf, "LogAnalyser");
//            FileSystem fs = FileSystem.get(conf);
//            job.setJarByClass(SessionBuildMapper.class);
//            ChainMapper.addMapper(job, SessionBuildMapper.class, LongWritable.class, Text.class, NullWritable.class, Text.class, conf);
//            FileInputFormat.addInputPath(job, new Path("D:\\2016-07-07\\mr1out"));
//            //output目录不允许存在。
//            Path output = new Path("D:\\2016-07-07\\mr1out\\end");
//            if (fs.exists(output)) {
//                fs.delete(output, true);
//            }
//            FileOutputFormat.setOutputPath(job, output);
//            boolean f = job.waitForCompletion(true);
//            if (f) {
//                System.out.println("job 执行成功");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
