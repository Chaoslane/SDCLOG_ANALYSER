package com.udbac.hadoop.mr;

import com.google.gson.Gson;
import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.util.IPv42AreaUtil;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import java.net.URLDecoder;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil{

    private Configuration conf;
    private String logFields;
    private String queryFields;

    LogParserUtil(Configuration conf) {
        this.conf = conf;
        this.initial();
    }

    private void initial() {
        logFields = conf.get("fields.log");
        queryFields = conf.get("fields.query");
        if (StringUtils.isBlank(logFields) || StringUtils.isBlank(queryFields)){
            System.out.println(LogConstants.INPUTARGSWARN);
            System.exit(-1);
        }
    }

    String handleLog(String[] lineSplits) throws IOException{
        return handleLog(lineSplits,this.logFields);
    }

    private String handleLog(String[] lineSplits, String logFields) throws IOException {
        SplitValueBuilder svb = new SplitValueBuilder(LogConstants.SEPARTIOR_TAB);
        //hashmap中放入除了query外所有字段
        Map<String, String> logMap = new HashMap<>();
        String date_time = TimeUtil.handleTime(
                lineSplits[0] + LogConstants.SEPARTIOR_SPACE + lineSplits[1]);
        logMap.put(LogConstants.LOG_COLUMN_DATETIME, date_time);
        logMap.put(LogConstants.LOG_COLUMN_IP, lineSplits[2]);
        logMap.put(LogConstants.LOG_COLUMN_REGION, IPv42AreaUtil.getArea(lineSplits[2]));
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
        //取得logFields参数指定的字段
        String[] fields = StringUtils.split(logFields, LogConstants.SEPARTIOR_COMMA);
        for (String field : fields) {
            svb.add(logMap.get(field));
        }
        return svb.toString();
    }

    String handleQuery(String[] lineSplits){
        return handleQuery(lineSplits,queryFields);
    }
    private String handleQuery(String[] lineSplits, String queryFields) {
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
                        value = URLDecoder.decode(URLDecoder.decode(value, "UTF-8"),"UTF-8");
                    } catch (UnsupportedEncodingException | IllegalArgumentException e) {
                        System.out.println("decode failed str:" + value);
                    }
                }
                queryMap.put(key, value);
            }
        }
        Gson gson = new Gson();
        if (queryFields.toLowerCase().equals("whole")) {
            //取得所有的query fields
            return gson.toJson(queryMap);
        }else {
            //取得queryFields参数指定的字段
            String[] fields = StringUtils.split(queryFields, LogConstants.SEPARTIOR_COMMA);
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
            return gson.toJson(selectedQuery);
        }
    }

}



