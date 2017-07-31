package com.udbac.hadoop.common;

/**
 * Created by root on 2017/7/27.
 */
public enum SdcField {

    DATE("date"),                  // date
    TIME("time"),                  // time
    IP("c_ip"),                    // c-ip
    USERNAME("cs_username"),       // cs-username
    HOST("cs_host"),               // cs-host
    METHOD("cs_method"),           // cs-method
    STEM("cs_uri_stem"),           // cs-uri-stem
    QUERY("cs_uri_query"),         // cs-uri-query
    STATUS("cs_status"),           // sc-status
    BYTES("cs_bytes"),             // sc-bytes
    VERSION("cs-version"),         // cs-version
    USERAGENT("cs_useragent"),     // cs(User-Agent)
    CSCOOKIE("cs_cookie"),         // cs(Cookie)
    REFERER("WT.referer"),         // cs(Referer)
    DCSID("dcsid");                // dcs-id

    private String key;

    SdcField() {}

    SdcField(String name) {
        this.key = name;
    }

    public String getKey() {
        return key;
    }


}
