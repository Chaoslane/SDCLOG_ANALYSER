package com.udbac.hadoop.entity;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.SplitValueBuilder;
import jodd.util.StringUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by root on 2016/7/22.
 */
public class SDCLog {
    //列
    private String date;
    private String time;
    private String cIp;
    private String sIp;
    private String csUriStem;
    private String csUserAgent;

    private HashMap<String, String> uriQuery;

    public HashMap<String, String> getUriQuery() {
        return uriQuery;
    }

    public void setUriQuery(HashMap<String, String> uriQuery) {
        this.uriQuery = uriQuery;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getcIp() {
        return cIp;
    }

    public void setcIp(String cIp) {
        this.cIp = cIp;
    }

    public String getsIp() {
        return sIp;
    }

    public void setsIp(String sIp) {
        this.sIp = sIp;
    }

    public String getCsUriStem() {
        return csUriStem;
    }

    public void setCsUriStem(String csUriStem) {
        this.csUriStem = csUriStem;
    }

    public String getCsUserAgent() {
        return csUserAgent;
    }

    public void setCsUserAgent(String csUserAgent) {
        this.csUserAgent = csUserAgent;
    }

    @Override
    public String toString() {
        String column = new SplitValueBuilder()
                .add(uriQuery.get(SDCLogConstants.LOG_QUERY_NAME_DEVICEID))
                .add(time)
                .add(date)
                .add(cIp)
                .add(csUserAgent)
                .add(uriQuery.get(SDCLogConstants.LOG_QUERY_NAME_UTMSOURCE))
                .add(uriQuery.get(SDCLogConstants.LOG_QUERY_NAME_WTUTYPE))
                .add(uriQuery.get(SDCLogConstants.LOG_QUERY_NAME_WTAVV))
                .add(uriQuery.get(SDCLogConstants.LOG_QUERY_NAME_WTPOS)).build();
        String event = new SplitValueBuilder()
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTLOGIN))
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTMENU))
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTCART))
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTUSER))
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTSUC))
                .add(uriQuery.get(SDCLogConstants.LOG_EVENT_NAME_WTPAY)).build();
        return column + "|" + event.replace("null", "").replace("|", "");
    }
}
