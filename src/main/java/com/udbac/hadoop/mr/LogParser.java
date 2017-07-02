package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParser extends Configured {
    private static Map<String, String> logMap = new HashMap<>(30);
    private static Logger logger = Logger.getLogger(LogParser.class);

    static Map<String, String> logParserSDC(String line) throws LogParseException {
        logMap.clear();
        String[] fields = line.split(" ");

        if (15 == fields.length) {
            handleRawFields(fields, 0);
        } else if (17 == fields.length) {
            handleRawFields(fields, 2);
        } else {
            throw new LogParseException(
                    "Unsupported Log Format:got " + fields.length + " fields, only support 15&17." + line);
        }

        // PC和终端日志 解析Cookie串，尝试获取CookieID和SessionID
        String ckid = null;     // CookieID
        String ssid = null;     // SessionID
        String ssms = "000";    // FIXME 毫秒时间暂时不取了

        String wtFPC = logMap.get("WT_FPC");
        if (StringUtils.isNotBlank(wtFPC)) {
            String[] info = wtFPC.replaceAll("[:;,]$", "").split("[=:;,]");

            if (info.length >= 6 && info[0].equals("id") && info[2].equals("lv") && info[4].equals("ss")
                    && info[3].length() == 13 && info[5].length() == 13) {
                // XXX lv和ss都是客户端时间
                // 从理论上讲，短时间内客户端与服务器的时钟差不会有显著变化
                // 所以在这里用客户端毫秒数作为服务器毫秒数
                String c_lv = info[3];
                String c_ss = info[5];

                //String c_id = null;
                //if (ckid.contains("!")) {
                //    c_id = info[1].split("!")[0];
                //} else {
                //    c_id = info[1];
                //}
                //SDC日志，Cookie字段中的用户ID，与WT.vtid、WT.co_f相同
                //logMap.put("WT_FPC.id", c_id);

                //SDC日志，Session最后一次访问时间，格式与WT_FPC.ss相同
                logMap.put("WT_FPC.lv", c_lv);
                //SDC日志，Session起始时间，time_t值后加3位毫秒值
                logMap.put("WT_FPC.ss", c_ss);

                if (StringUtils.isNumeric(c_ss) && StringUtils.isNumeric(c_lv)) {
                    Long ss = Long.parseLong(c_ss);
                    Long lv = Long.parseLong(c_lv);
                    //Session存活时间
                    logMap.put("SS.live", String.valueOf((lv - ss) / 1000));
                }
                ckid = info[1];
                ssid = StringUtils.isNotBlank(ckid) ? ckid + ":" + c_ss : ssid;
            }
        }
        // 未能成功的从WT_FPC中解析出CookieID 和SessionID，尝试从cs_uri_query中解析
        if (StringUtils.isBlank(ckid)) {
            for (String k : ckids) {
                if (StringUtils.isNotBlank(logMap.get(k))) {
                    ckid = logMap.get(k);
                    if (StringUtils.isNotBlank(logMap.get("WT.vt_sid"))) {
                        ssid = logMap.get("WT.vt_sid");
                    } else if (StringUtils.isNotBlank(logMap.get("WT.vtvs"))) {
                        ssid = ckid + ":" + logMap.get("WT.vtvs");
                    }
                    break;
                }
            }
        }

        // XXX 在SDC日志中，实际上并不区分CookieID和SessionID
        // 实际上CookieID的尾部就是 第一次访问时间
        // FIXME 需要再次确认
        if (StringUtils.isNotBlank(ckid)) {
            logMap.put("ckid", ckid);
            logMap.put("ssid", ssid);
            logMap.remove("WT.vtid");
            logMap.remove("WT.co_f");
            logMap.remove("WT.vt_sid");
            logMap.remove("WT.vtvs");
        }
        return logMap;
    }

    private static void handleRawFields(String[] fields, int offset) throws LogParseException {
        logMap.put(dateTime,
                TimeUtil.handleTime(fields[offset] + " " + fields[1 + offset]));
        logMap.put(ip, fields[2 + offset]);
        logMap.put(userName, fields[3 + offset]);
        logMap.put(host, fields[4 + offset]);
        logMap.put(method, fields[5 + offset]);
        logMap.put(stem, fields[6 + offset]);
        logMap.put(status, fields[8 + offset]);
        logMap.put(bytes, fields[9 + offset]);
        logMap.put(version, fields[10 + offset]);
        logMap.put(userAgent, fields[11 + offset]);
        logMap.put(referer, fields[13 + offset]);
        if (fields[14 + offset].length() == 30) {
            logMap.put(dcsid, fields[14 + offset].substring(26));
            logMap.put(dcsid_l, fields[14 + offset]);
        }
        // 拆解cs_uri_query、cs_cookie，生成参数表
        handleQuery(fields[7 + offset], "&");
        handleQuery(fields[12 + offset], ";+");
    }


    //解析query WT_FPC中的key value值，有必要的进行url解码
    private static void handleQuery(String query, String delimiter) {
        if (StringUtils.isNotBlank(query)) {
            String[] items = StringUtils.split(query, delimiter);
            for (String item : items) {
                String[] kv = item.split("=", 2);
                if (kv.length == 2 && StringUtils.isNotEmpty(kv[1])) {
                    String key = kv[0].replaceAll("(wt|Wt|wT)", "WT");
                    String value = kv[1].matches(".*((?:%[a-zA-Z\\d]{2})+).*") ? urlDecode(kv[1]) : kv[1];
                    logMap.put(key, StringUtils.isEmpty(value) ? null : value);
                }
            }
        }
    }

    //URL解码
    private static String urlDecode(String strUrl) {
        try {
            strUrl = URLDecoder.decode(
                    strUrl.replace("\\x", "%").replace("%25", "%"), "utf-8");
        } catch (Exception e) {
            logger.warn("URL decode error :" + strUrl);
            if (strUrl.contains("http"))
                strUrl = strUrl.split("\\?", 2)[0];
            else strUrl = null;
        }
        return strUrl;
    }


    private static final String dateTime = "date_time";
    private static final String ip = "c_ip";
    private static final String userName = "cs_username";
    private static final String host = "cs_host";
    private static final String method = "cs_method";
    private static final String stem = "cs_uri_stem";
    private static final String status = "cs_status";
    private static final String bytes = "cs_bytes";
    private static final String version = "cs_version";
    private static final String userAgent = "cs_useragent";
    private static final String referer = "WT.referer";
    private static final String dcsid = "dcsid";
    private static final String dcsid_l = "dcsid_l";
    // cookieid
    private static final String[] ckids = {"WT.vtid", "WT.co_f"};

}



