package com.udbac.test;

import com.udbac.hadoop.common.LogParseException;
import com.udbac.hadoop.mr.LogParser;
import org.apache.commons.io.IOUtils;
import org.mortbay.util.MultiMap;
import org.mortbay.util.UrlEncoded;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;

/**
 * Created by root on 2017/3/31.
 */
public class SplitTest {
    public static void main(String[] args) throws LogParseException, MalformedURLException {
        String url = LogParser.urlDecode("http%253A%252F%252Fsearch.10086.cn%252Fsearch%253Fcontent%253D%25E7%259F%25AD%25E5%258F%25B7%2526areacode%253Dsc%2526areaName%253D%2523");
        URL u = new URL(url);
        String query = u.getQuery();
//        MultiMap values = new MultiMap();
//        UrlEncoded.decodeTo(query, values, "UTF-8", 1000);
        String c_ss = "1231231242314125314s656";
        String c_lv ="134613461345654756869780";
        if (c_ss.matches("^\\d[0-9]*\\d$") && c_lv.matches("^\\d[0-9]*\\d$")) {
            System.out.println(1);
        }
    }
}
