package com.udbac.hadoop.common;

import com.udbac.hadoop.mr.LogAnalyserRunner;
import com.udbac.hadoop.util.TimeUtil;
import com.udbac.hadoop.util.URLDecodeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParser extends Configured {
    private static Map<String, String> logMap = new HashMap<>(30);
    private static Logger logger = Logger.getLogger(LogParser.class);

    public static Map<String, String> logParserSDC(String line) throws LogParseException {
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

    /**
     * 取得日志字段数组，拆解字符串，放入hashmap
     * @param fields
     * @param offset 日志数组索引偏移
     * @throws LogParseException
     */
    private static void handleRawFields(String[] fields, int offset) throws LogParseException {

        for (SdcField sdcField : SdcField.values()) {
            int index = sdcField.ordinal();
            if (index != SdcField.QUERY.ordinal() && index != SdcField.CSCOOKIE.ordinal()) {
                logMap.put(sdcField.getKey(), fields[index + offset]);
            }
        }
        // 纠正日志日期时区，并验证日期合法
        String dateTime = TimeUtil.handleTime(
                logMap.get(SdcField.DATE.getKey()) + " " + logMap.get(SdcField.TIME.getKey()));

        if (!dateTime.contains(LogAnalyserRunner.getLogDate())) {
            throw new LogParseException("Error date, the log date must be:" + LogAnalyserRunner.getLogDate());
        }

        logMap.put("date_time", dateTime);
        // 拆解cs_uri_query、cs_cookie，生成参数表
        handleQuery(fields[SdcField.QUERY.ordinal() + offset], "&");
        handleQuery(fields[SdcField.CSCOOKIE.ordinal() + offset], ";+");
    }


    //解析query WT_FPC中的key value值，有必要的进行url解码
    private static void handleQuery(String query, String delimiter) {
        if (StringUtils.isNotBlank(query)) {
            String[] items = StringUtils.split(query, delimiter);
            for (String item : items) {
                String[] kv = item.split("=", 2);
                if (kv.length == 2 && StringUtils.isNotEmpty(kv[1])) {
                    String key = kv[0].replaceAll("(wt|Wt|wT)", "WT");
                    String value = URLDecodeUtil.decodeSafe(kv[1]);
                    logMap.put(key, StringUtils.isEmpty(value) ? null : value);
                }
            }
        }
    }

    // cookieid
    private static final String[] ckids = {"WT.vtid", "WT.co_f"};

}



