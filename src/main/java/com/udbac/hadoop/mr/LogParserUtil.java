package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.util.IPv4Handler;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {

    public static String handleLog(String[] lineSplits, String[] fields) {
        SplitValueBuilder svb = new SplitValueBuilder();
        Map<String, String> allfields = handleLogMap(lineSplits);
        for (String field : fields) {
            String muiltFeild = null;
            String like = null;
            if (field.contains(":")) {
                String[] muiltFeildAndLike = StringUtils.split(field, ":");
                muiltFeild = muiltFeildAndLike[0];
                like = muiltFeildAndLike[1];
                field = muiltFeild;
            }
            if (StringUtils.isNotBlank(field) && field.contains("?")) {
                String[] fiesplits = StringUtils.split(field, "?");
                for (String fiesplit : fiesplits) {
                    if (StringUtils.isNotBlank(allfields.get(fiesplit))) {
                        field = fiesplit;
                        break;
                    }
                }
            }
            if (StringUtils.isNotBlank(like)) {
                if (StringUtils.isBlank(allfields.get(field)) || !allfields.get(field).contains(like)) {
                    return null;
                }
            }
            svb.add(allfields.get(field));
        }
        return svb.toString();
    }

    private static Map<String, String> handleLogMap(String[] lineSplits) {
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
                        System.out.println("URLDecoder parse error");
                    }
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


