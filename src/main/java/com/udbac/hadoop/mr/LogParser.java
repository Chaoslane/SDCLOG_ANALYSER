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
        //logMap.put("cs_username", fields[3]);
        //logMap.put("cs_host", fields[4]);
        //logMap.put("cs_method", fields[5]);
        //logMap.put("sc_status", fields[8]);
        //logMap.put("sc_bytes", fields[9]);
        //logMap.put("cs_version", fields[10]);
        //logMap.put("cs_cookie", fields[12]);

        // 拆解cs_uri_query、cs_cookie，生成参数表
        String query = fields[7];
        handleQuery(query, "&");
        String cookie = fields[12].replace(";;", ";+");
        handleQuery(cookie, ";+");

        // PC和终端日志 解析Cookie串，尝试获取CookieID和SessionID
        String ckid = null;     // CookieID
        String ssid = null;     // SessionID
        String ssms = "000";    // FIXME 毫秒时间暂时不取了

        String wtFPC = logMap.get("WT_FPC");
        if (StringUtils.isNotBlank(wtFPC)) {
            wtFPC = urlDecode(wtFPC);
            String[] info = wtFPC.replaceAll("[:;,]$", "").split("[=:;,]");

            if (info.length >= 6 && info[0].equals("id") && info[2] .equals("lv")  && info[4] .equals("ss")
                    && info[3].length() == 13 && info[5].length() == 13) {

                ckid = info[1];

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
                    ssid = ckid + ":" + logMap.get("WT.vtvs");
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
        }


        check();
        return logMap;
    }


    private static void check() {

        logMap.put("prov", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[0]);
        logMap.put("city", IPv42AreaUtil.getArea(logMap.get("c_ip")).split(",")[1]);
        logMap.remove("c_ip");

        // "WT.es", "WT.referer" 去掉参数部分
        for (String key : new String[]{"WT.es", "WT.referer"}) {
            String value = logMap.get(key);
            if (StringUtils.isNotBlank(value)) {
                value = value.split("\\?", 2)[0];
                logMap.put(key, value);
            }
        }

//        String ckid = logMap.get("WT_FPC.id");
//        if (StringUtils.isNotBlank(ckid) && ckid.length() >= 32) {
//            logMap.put("WT_FPC.id_hash", ckid.substring(0, 19)); //Cookie中解析出的用户ID的hash值
//            logMap.put("WT_FPC.id_tick", ckid.substring(19)); //Cookie中解析出的Cookie创建时间
//        }
    }

    //解析query WT_FPC中的key value值，有必要的进行url解码
    private static void handleQuery(String query, String delimiter) {
        if (StringUtils.isNotBlank(query)){
            String[] items = StringUtils.split(query, delimiter);
            for (String item : items) {
                String[] kv = item.split("=", 2);
                if (kv.length == 2) {
                    String key = kv[0].replaceAll("(wt|Wt|wT)","WT");
                    String value = kv[1].matches(".*(\\\\x[A-Za-z0-9]{2})+.*|.*(%[A-Za-z0-9]{2})+.*") ? urlDecode(kv[1]) : kv[1];
                    logMap.put(key, value);
                }
            }
        }
    }

    //URL解码
    public static String urlDecode(String strUrl){
        try {
            strUrl = strUrl.replace("\\x", "%").replace("%25", "%");
            strUrl = URLDecoder.decode(strUrl, "utf-8");
        } catch (Exception e ) {
            e.printStackTrace();
        }
        return strUrl;
    }

}



