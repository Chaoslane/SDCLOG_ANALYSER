package com.udbac.hadoop.mr;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParser {
    private static Map<String, String> logMap = new HashMap<>(100);
    private static Logger logger = Logger.getLogger(LogParser.class);

    static Map<String, String> logParserSDC(String line) throws LogParseException {
        logMap.clear();
        String[] fields = line.split(" ", -1);

        if (15 == fields.length) {
            // SDC日志采用格林威治时间 调整时区
            String dateTime = TimeUtil.handleTime(fields[0] + " " + fields[1]);
            logMap.put("date_time", dateTime);
            logMap.put("c_ip", fields[2]);
            logMap.put("cs_username", fields[3]);
            logMap.put("cs_host", fields[4]);
            logMap.put("cs_method", fields[5]);
            logMap.put("cs_uri_stem", fields[6]);
            logMap.put("sc_status", fields[8]);
            logMap.put("sc_bytes", fields[9]);
            logMap.put("cs_version", fields[10]);
            logMap.put("cs_useragent", fields[11]);
            logMap.put("WT.referer", fields[13]);
            if (fields[14].length() == 30)
                logMap.put("dcsid", fields[14].substring(26));

            // 拆解cs_uri_query、cs_cookie，生成参数表
            handleQuery(fields[7], "&");
            handleQuery(fields[12], ";+");
        }else if (17 == fields.length){
            String dateTime = TimeUtil.handleTime(fields[2] + " " + fields[3]);
            logMap.put("date_time", dateTime);
            logMap.put("c_ip", fields[4]);
            logMap.put("cs_username", fields[5]);
            logMap.put("cs_host", fields[6]);
            logMap.put("cs_method", fields[7]);
            logMap.put("cs_uri_stem", fields[8]);
            logMap.put("sc_status", fields[10]);
            logMap.put("sc_bytes", fields[11]);
            logMap.put("cs_version", fields[12]);
            logMap.put("cs_useragent", fields[13]);
            logMap.put("WT.referer", fields[15]);
            if (fields[14].length() == 30)
                logMap.put("dcsid", fields[14].substring(26));

            // 拆解cs_uri_query、cs_cookie，生成参数表
            handleQuery(fields[9], "&");
            handleQuery(fields[14], ";+");
        } else{
            if (fields[0].contains("#")) {
                throw new LogParseException(
                        "Skip log comments:" + line);
            } else {
                throw new LogParseException(
                        "Unsupported Log Format:got " + fields.length + " fields, only support 15&17." + line);
            }
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
                //logMap.put("WT_FPC.lv", c_lv);
                //SDC日志，Session起始时间，time_t值后加3位毫秒值
                //logMap.put("WT_FPC.ss", c_ss);

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
            for (String k : new String[]{"WT.vtid", "WT.co_f"}) {
                if (StringUtils.isNotBlank(logMap.get(k))) {
                    ckid = logMap.get(k);
                    if (StringUtils.isNotBlank(logMap.get("WT.vt_sid"))) {
                        ssid = logMap.get("WT.vt_sid");
                    } else {
                        ssid = StringUtils.isNotBlank(logMap.get("WT.vtvs")) ? ckid + ":" + logMap.get("WT.vtvs") : ssid;
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

        check();
        return logMap;
    }


    private static void check() {

//        logMap.put("prov", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[0]);
//        logMap.put("city", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[1]);
//        logMap.remove("c_ip");

//        String ckid = logMap.get("WT_FPC.id");
//        if (StringUtils.isNotBlank(ckid) && ckid.length() >= 32) {
//            logMap.put("WT_FPC.id_hash", ckid.substring(0, 19)); //Cookie中解析出的用户ID的hash值
//            logMap.put("WT_FPC.id_tick", ckid.substring(19)); //Cookie中解析出的Cookie创建时间
//        }

        // "WT.es", "WT.referer" 去掉参数部分
        for (String key : new String[]{"WT.es", "WT.referer"}) {
            String value = logMap.get(key);
            if (StringUtils.isNotBlank(value)) {
                value = value.split("\\?", 2)[0];
                logMap.put(key, value);
            }
        }
    }

    //解析query WT_FPC中的key value值，有必要的进行url解码
    private static void handleQuery(String query, String delimiter) {
        if (StringUtils.isNotBlank(query)) {
            query = query.replaceAll("%3D|%3d", "=");
            String[] items = StringUtils.split(query, delimiter);
            for (String item : items) {
                String[] kv = item.split("=", 2);
                if (kv.length == 2) {
                    String key = kv[0].replaceAll("(wt|Wt|wT)", "WT");
                    String value = urlDecode(kv[1]);
                    logMap.put(key, value);
                }
            }
        }
    }

    //URL解码
    private static String urlDecode(String strUrl) {
        try {
            strUrl = strUrl.replace("\\x", "%").replace("%25", "%");
            strUrl = URLDecoder.decode(strUrl, "utf-8");
            return strUrl;
        } catch (Exception e) {
            logger.warn("URL decode error :" + strUrl);
            if (strUrl.length() > 50) strUrl = "";
            return strUrl;
        }
    }

    /**
     * 取程序输入参数 从logmap中取的参数字段值 取fieldsColumn差集放入Json
     *
     * @param logMap 一条日志的所有字段值
     * @return 拼接结果字符串
     */
    public static String getResStr(Map<String, String> logMap , String[] fieldsColumn) {
        SplitValueBuilder svb = new SplitValueBuilder("\t");
        for (String field : fieldsColumn) {
            String value = "";
            if (StringUtils.isNotBlank(logMap.get(field))) {
                value = logMap.get(field);
            }
            svb.add(value);
            logMap.remove(field);
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        svb.add(gson.toJson(logMap));
        return svb.toString();
    }

}



