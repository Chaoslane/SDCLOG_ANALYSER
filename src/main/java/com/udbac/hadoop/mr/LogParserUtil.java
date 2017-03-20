package com.udbac.hadoop.mr;

import com.google.gson.Gson;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {
    private static Parser uapaser;
    private static IPv4Handler iPv4Handler;

    static {
        try {
            uapaser = new Parser();
            iPv4Handler = new IPv4Handler();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static String handleLog(String[] lineSplits,String fieldsLog) throws IOException {
        SplitValueBuilder svb = new SplitValueBuilder(LogConstants.SEPARTIOR_TAB);
        //hashmap中放入除了query外所有字段
        Map<String, String> logMap = new HashMap<>();
        String date_time = TimeUtil.handleTime(
                lineSplits[0] + LogConstants.SEPARTIOR_SPACE + lineSplits[1]);
        logMap.put(LogConstants.LOG_COLUMN_DATETIME, date_time);
        logMap.put(LogConstants.LOG_COLUMN_IP, lineSplits[2]);
        logMap.put(LogConstants.LOG_COLUMN_REGION, iPv4Handler.getArea(lineSplits[2]));
        logMap.put(LogConstants.LOG_COLUMN_USERNAME, lineSplits[3]);
        logMap.put(LogConstants.LOG_COLUMN_HOST, lineSplits[4]);
        logMap.put(LogConstants.LOG_COLUMN_METHOD, lineSplits[5]);
        logMap.put(LogConstants.LOG_COLUMN_URISTEM, lineSplits[6]);
        logMap.put(LogConstants.LOG_COLUMN_STATUS, lineSplits[8]);
        logMap.put(LogConstants.LOG_COLUMN_BYTES, lineSplits[9]);
        logMap.put(LogConstants.LOG_COLUMN_VERSION, lineSplits[10]);
        logMap.put(LogConstants.LOG_COLUMN_USERAGENT, lineSplits[11]);
        logMap.put(LogConstants.LOG_COLUMN_COOKIE, lineSplits[12]);
        logMap.put(LogConstants.LOG_COLUMN_REFERER, lineSplits[13]);
        logMap.put(LogConstants.LOG_COLUMN_DCSID, lineSplits[14]);
        //取得fieldsLog参数指定的字段
        String[] fields = StringUtils.split(fieldsLog, LogConstants.SEPARTIOR_SPACE);
        for (String field : fields) {
            svb.add(logMap.get(field));
        }
        return svb.toString();
    }

    protected static String handleQuery(String[] lineSplits, String fieldsQuery) {
        String query = lineSplits[7];
        String key = null;
        String value = null;
        Map<String, String> queryMap = new HashMap<>();
        String[] uriQuerys = StringUtils.split(query, LogConstants.SEPARATOR_AND);
        for (String uriQuery : uriQuerys) {
            String[] uriitems = StringUtils.split(uriQuery, LogConstants.SEPARTIOR_EQUAL);
            if (uriitems.length == 2) {
                key = uriitems[0];
                value = uriitems[1];
                if (value.contains("%")) {
                    try {
                        value = URLDecoder.decode(value, "UTF-8");
                    } catch (UnsupportedEncodingException | IllegalArgumentException e) {

                    }
                }
                queryMap.put(key, value);
            }
        }
        //取得fieldsQuery参数指定的字段
        String[] fields = StringUtils.split(fieldsQuery, LogConstants.SEPARTIOR_COMMA);
        Map<String, String> selectedQuery = new HashMap<>();
        for (String field : fields) {
            if (field.contains(LogConstants.SEPARTIOR_QUES)) {
                String[] fiesplits = StringUtils.split(field, LogConstants.SEPARTIOR_QUES);
                for (String fiesplit : fiesplits) {
                    if (StringUtils.isNotBlank(queryMap.get(fiesplit))) {
                        selectedQuery.put(fiesplit, queryMap.get(fiesplit));
                        break;
                    }
                }
            }else {
                selectedQuery.put(field, queryMap.get(field));
            }
        }
        Gson gson = new Gson();
        return gson.toJson(selectedQuery);
//                        try {
//                            if (uriitems[1].endsWith("%")) {
//                                uriitems[1] = URLDecoder.decode(uriitems[1].substring(0, uriitems[1].length() - 1), "UTF-8");
//                            } else if (uriitems[1].length() - uriitems[1].lastIndexOf("%") == 2) {
//                                uriitems[1] = URLDecoder.decode(uriitems[1].substring(0, uriitems[1].length() - 2), "UTF-8");
//                            }
//                        } catch (UnsupportedEncodingException |IllegalArgumentException e1) {
//                            System.out.println("URLDecoder parse error~! uricode:" + uriitems[1]);
//                        }
    }

}


