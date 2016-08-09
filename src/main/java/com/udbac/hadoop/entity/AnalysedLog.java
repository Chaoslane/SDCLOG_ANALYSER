package com.udbac.hadoop.entity;

import com.udbac.hadoop.util.SplitValueBuilder;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2016/7/27.
 */
public class AnalysedLog {
    private String visitId;
    private String deviceId;
    private String time;
    private String date;
    private String cIp;
    private String csUserAgent;
    private String utmSource;
    private String uType;
    private String wtAvv;
    private String wtPos;
    //事件的集合
    private Map<String, String> eveMap = new HashMap<>();
    //时长
    private BigDecimal duration;
    //每一条都是1
    private String pageView;

    public String getVisitId() {
        return visitId;
    }

    public void setVisitId(String visitId) {
        this.visitId = visitId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getcIp() {
        return cIp;
    }

    public void setcIp(String cIp) {
        this.cIp = cIp;
    }

    public String getCsUserAgent() {
        return csUserAgent;
    }

    public void setCsUserAgent(String csUserAgent) {
        this.csUserAgent = csUserAgent;
    }

    public String getUtmSource() {
        return utmSource;
    }

    public void setUtmSource(String utmSource) {
        this.utmSource = utmSource;
    }

    public String getuType() {
        return uType;
    }

    public void setuType(String uType) {
        this.uType = uType;
    }

    public String getWtAvv() {
        return wtAvv;
    }

    public void setWtAvv(String wtAvv) {
        this.wtAvv = wtAvv;
    }

    public String getWtPos() {
        return wtPos;
    }

    public void setWtPos(String wtPos) {
        this.wtPos = wtPos;
    }

    public Map<String, String> getEveMap() {
        return eveMap;
    }

    public void setEveMap(Map<String, String> eveMap) {
        this.eveMap = eveMap;
    }

    public BigDecimal getDuration() {
        return duration;
    }

    public void setDuration(BigDecimal duration) {
        this.duration = duration;
    }

    public String getPageView() {
        return pageView;
    }

    public void setPageView(String pageView) {
        this.pageView = pageView;
    }

    @Override
    public String toString() {
        SplitValueBuilder svb = new SplitValueBuilder();
        if (eveMap.containsKey("login"))
            svb.add("1");
        else svb.add("0");
        if (eveMap.containsKey("menu"))
            svb.add("1");
        else svb.add("0");
        if (eveMap.containsKey("user"))
            svb.add("1");
        else svb.add("0");
        if (eveMap.containsKey("cart"))
            svb.add("1");
        else svb.add("0");
        if (eveMap.containsKey("suc"))
            svb.add("1");
        else svb.add("0");
        if (eveMap.containsKey("pay"))
            svb.add("1");
        else svb.add("0");

        return new SplitValueBuilder()
                .add(visitId)
                .add(deviceId)
                .add(time)
                .add(date)
                .add(cIp)
                .add(csUserAgent)
                .add(utmSource)
                .add(uType)
                .add(wtAvv)
                .add(wtPos)
                .add(svb.build())
                .add(duration)
                .add(pageView).build();
    }
}
