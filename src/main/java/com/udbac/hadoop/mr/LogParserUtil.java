package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.util.IPv4Handler;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import ua_parser.Parser;
import ua_parser.Client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {
    private static Parser uapaser;
    static {
        try {
            uapaser = new Parser();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * fields 插码字段可能为多个，比如WT.mobile或mobile 用?进行分隔
     *
     * @param lineSplits 切割后的日志数组
     * @param fields     切割后的fileds数组
     * @return 返回最终的清洗结果
     */
    public static String handleLog(String[] lineSplits, String[] fields) throws IOException {
        SplitValueBuilder svb = new SplitValueBuilder(LogConstants.SEPARTIOR_TAB);
        Map<String, String> allfields = handleLogMap(lineSplits);
        for (String field : fields) {
            if (field.contains(LogConstants.SEPARTIOR_QUES)) {
                String[] fiesplits = StringUtils.split(field, LogConstants.SEPARTIOR_QUES);
                for (String fiesplit : fiesplits) {
                    if (StringUtils.isNotBlank(allfields.get(fiesplit))) {
                        field = fiesplit;
                        break;
                    }
                }
            }
            svb.add(allfields.get(field));
        }
        return svb.toString();
    }

    /**
     * 处理所有日志数组进行解析后，放入一个hashmap
     *
     * @param lineSplits 日志数组
     * @return 返回全量的日志信息
     */
    private static Map<String, String> handleLogMap(String[] lineSplits) throws IOException {
        Map<String, String> logMap = new HashMap<>();
        String date_time = TimeUtil.handleTime(lineSplits[0] + " " + lineSplits[1]);
        logMap.put(LogConstants.LOG_COLUMN_DATETIME, date_time);
        logMap.put(LogConstants.LOG_COLUMN_IP, lineSplits[2]);
        handleIP(logMap, lineSplits[2]);
        logMap.put(LogConstants.LOG_COLUMN_USERNAME, lineSplits[3]);
        logMap.put(LogConstants.LOG_COLUMN_HOST, lineSplits[4]);
        logMap.put(LogConstants.LOG_COLUMN_METHOD, lineSplits[5]);
        logMap.put(LogConstants.LOG_COLUMN_URISTEM, lineSplits[6]);
        handleQuery(logMap, lineSplits[7]);
        logMap.put(LogConstants.LOG_COLUMN_STATUS, lineSplits[8]);
        logMap.put(LogConstants.LOG_COLUMN_BYTES, lineSplits[9]);
        logMap.put(LogConstants.LOG_COLUMN_VERSION, lineSplits[10]);
        handleUA(logMap, lineSplits[11]);
        logMap.put(LogConstants.LOG_COLUMN_COOKIE, lineSplits[12]);
        logMap.put(LogConstants.LOG_COLUMN_REFERER, lineSplits[13]);
        logMap.put(LogConstants.LOG_COLUMN_DCSID, lineSplits[14]);
        return logMap;
    }

    private static void handleQuery(Map<String, String> logMap, String query) {
        String[] uriQuerys = StringUtils.split(query, LogConstants.SEPARATOR_AND);
        for (String uriQuery : uriQuerys) {
            String[] uriitems = StringUtils.split(uriQuery, LogConstants.SEPARTIOR_EQUAL);
            if (uriitems.length == 2) {
                if (uriitems[1].contains("%")) {
                    try {
                        uriitems[1] = URLDecoder.decode(uriitems[1], "UTF-8");
                    } catch (UnsupportedEncodingException | IllegalArgumentException e) {
                        System.out.println("URLDecoder parse error~! uricode:" + uriitems[1]);
                    }
                }
                logMap.put(uriitems[0], uriitems[1]);
            }
        }
    }

    private static void handleUA(Map<String, String> logMap, String uaString) throws IOException {
        if (StringUtils.isNotBlank(uaString)) {
            Client c = uapaser.parse(uaString);
            logMap.put(LogConstants.UA_OS_NAME, c.os.family);
            logMap.put(LogConstants.UA_OS_VERSION, c.os.major + "." + c.os.minor);
            logMap.put(LogConstants.UA_BROWSER_NAME, c.userAgent.family);
            logMap.put(LogConstants.UA_BROWSER_NAME_VERSION, c.userAgent.major + "." + c.userAgent.minor);

        }
    }

    private static void handleIP(Map<String, String> logMap, String ip) {
        if (StringUtils.isNotBlank(ip) && ip.length() > 8) {
            String[] regioninfo = IPv4Handler.getArea(ip);
            logMap.put(LogConstants.REGION_PROVINCE, regioninfo[0]);
            logMap.put(LogConstants.REGION_CITY, regioninfo[1]);
            logMap.put(LogConstants.LOG_COLUMN_IPCODE, IPv4Handler.getIPcode(ip));
        }
    }
}


