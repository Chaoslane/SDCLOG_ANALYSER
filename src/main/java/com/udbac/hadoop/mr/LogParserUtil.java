package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.util.IPv4Handler;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {
    private final static Logger logger = Logger.getLogger(LogParserUtil.class);

    public static String handleLog(String[] tokens, String[] keys) {
        SplitValueBuilder svb = new SplitValueBuilder();
        for (String key : keys) {
            svb.add(handleLogMap(tokens).get(key));
        }
        return svb.toString();
    }

    public static Map<String, String> handleLogMap(String[] lineSplits) {
        Map<String, String> logMap = new HashMap<>();
        String date_time = TimeUtil.handleTime(lineSplits[0] +" "+ lineSplits[1]);
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
        String[] uriQuerys = StringUtils.split(query, LogConstants.QUERY_SEPARTIOR);
        for (String uriQuery : uriQuerys) {
            String[] uriitems = StringUtils.split(uriQuery, "=");
            if (uriitems.length == 2) {
                try {
                    uriitems[1] = URLDecoder.decode(uriitems[1], "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    logger.error("url解析异常" + e);
                    e.printStackTrace();
                }
                logMap.put(uriitems[0], uriitems[1]);
            }
        }
    }

    private static void handleUA(Map<String, String> logMap, String usString) {
        if (StringUtils.isNotBlank(usString)) {
            UserAgent userAgent = UserAgent.parseUserAgentString(usString);
            logMap.put(LogConstants.UA_OS_NAME, userAgent.getOperatingSystem().getName());
            logMap.put(LogConstants.UA_BROWSER_NAME, userAgent.getBrowser().getName());
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


