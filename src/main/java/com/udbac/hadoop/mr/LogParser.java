package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.util.IPv42AreaUtil;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParser extends Configured {
    private static Map<String, String> logMap = new HashMap<>(100);

    static Map<String, String> logParserSDC(String line) throws LogParseException {
        logMap.clear();
        String[] fields = line.split("[\t ]");

        if (15 != fields.length) {
            if (fields[0].contains("#")) {
                throw new LogParseException(
                        "Skip log comments:" + line);
            }else{
                throw new LogParseException(
                        "Unsupported Log Format:got " + fields.length + " fields, only support 15." + line);
            }
        }

        // SDC日志采用格林威治时间 调整时区
        String dateTime = TimeUtil.handleTime(fields[0] + " " + fields[1]);
        logMap.put("datetime", dateTime);
        logMap.put("c_ip", fields[2]);
        if (fields[6].length()>1) logMap.put("cs_uri_stem", fields[6]);
        if (fields[11].length()>1) logMap.put("cs_useragent", fields[11]);
        if (fields[13].length()>1) logMap.put("WT.referer", fields[13]);
        if (fields[14].length()==30)logMap.put("dcsid", fields[14].substring(26));
//        logMap.put("cs_username", fields[3]);
//        logMap.put("cs_host", fields[4]);
//        logMap.put("cs_method", fields[5]);
//        logMap.put("sc_status", fields[8]);
//        logMap.put("sc_bytes", fields[9]);
//        logMap.put("cs_version", fields[10]);
//        logMap.put("cs_cookie", fields[12]);

        // 拆解cs_uri_query、cs_cookie，生成参数表
        String query = fields[7];
        handleQuery(query, "&");
        String cookie = fields[12].replace(";;", ";+");
        handleQuery(cookie, ";+");

        // PC和终端日志 解析Cookie串，尝试获取CookieID和SessionID
        String ckid = null;     // CookieID
        String ssms = "000";    // FIXME 毫秒时间暂时不取了

        String wtFPC = logMap.get("WT_FPC");
        if (StringUtils.isNotBlank(wtFPC)) {
            wtFPC = urlDecode(wtFPC);
            String[] info = wtFPC.replaceAll("[:;,]$", "").split("[=:;,]");

            if (info.length >= 6 && info[0].equals("id") && info[2] .equals("lv")  && info[4] .equals("ss")
                    && info[3].length() == 13 && info[5].length() == 13) {
                String cms = info[3].substring(info[3].length()-3);

                ckid = info[1];
                ssms = cms;

                String c_id = null;
                if (ckid.contains("!")) {
                    c_id = info[1].split("!")[0];
                } else {
                    c_id = info[1];
                }

                // XXX lv和ss都是客户端时间
                // 从理论上讲，短时间内客户端与服务器的时钟差不会有显著变化
                // 所以在这里用客户端毫秒数作为服务器毫秒数
                String c_lv = info[3];
                String c_ss = info[5];

                //SDC日志，Cookie字段中的用户ID，与WT.vtid、WT.co_f相同
                //logMap.put("WT_FPC.id", c_id);
                //SDC日志，Session最后一次访问时间，格式与WT_FPC.ss相同
                //logMap.put("WT_FPC.lv", c_lv);
                //SDC日志，Session起始时间，time_t值后加3位毫秒值
                //logMap.put("WT_FPC.ss", c_ss);

                logMap.put("ssid", ckid + ":" + c_ss);

                if (StringUtils.isNumeric(c_ss) && StringUtils.isNumeric(c_lv)) {
                    Long ss = Long.parseLong(c_ss);
                    Long lv = Long.parseLong(c_lv);
                    //Session存活时间
                    logMap.put("SS.live", String.valueOf((lv - ss) / 1000));
                }
            }
        }

        // 未能成功的从WT_FPC中解析出CookieID 和SessionID，尝试从cs_uri_query中解析
        if (StringUtils.isBlank(ckid)) {
            for (String k : new String[]{"WT.vtid", "WT.co_f"}) {
                if (StringUtils.isNotBlank(logMap.get(k))) {
                    ckid = logMap.get(k);
                    break;
                }
            }
        }

        if (StringUtils.isNotBlank(ckid)) {
            // XXX 在SDC日志中，实际上并不区分CookieID和SessionID
            // 实际上CookieID的尾部就是Session起始时间
            // FIXME 需要再次确认
            logMap.put("ckid", ckid);
        }

        check();
        return logMap;
    }


    private static void check() {

        logMap.put("prov", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[0]);
        logMap.put("city", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[1]);
        logMap.remove("c_ip");

        String ckid = logMap.get("WT_FPC.id");
        if (StringUtils.isNotBlank(ckid) && ckid.length() >= 32) {
            logMap.put("WT_FPC.id_hash", ckid.substring(0, 19)); //Cookie中解析出的用户ID的hash值
            logMap.put("WT_FPC.id_tick", ckid.substring(19)); //Cookie中解析出的Cookie创建时间
        }

        for (String s : new String[]{"WT.es", "WT.referer","WT.event"}) {
            if (StringUtils.isNotBlank(logMap.get(s))) {
                logMap.put(s, urlDecode(logMap.get(s)));
            }
        }
    }

    private static void handleQuery(String query, String delimiter) {
        String[] items = StringUtils.split(query, delimiter);
        for (String item : items) {
            String[] kv = item.split("=", 2);
            String key = kv[0];
            if (StringUtils.containsIgnoreCase(kv[0], "wt.")) {
                key = kv[0].replace("wt.", "WT.");
            }
            if (kv.length == 2) {
                logMap.put(key, kv[1]);
            }
        }
    }

    //URL解码
    public static String urlDecode(String strUrl){
        try {
            return URLDecoder.decode(strUrl.replace("\\x", "%").replace("%25", "%"), "UTF-8");
        } catch (Exception e ) {
            return "";
        }
    }

}



