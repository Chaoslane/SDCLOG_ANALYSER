package com.udbac.hadoop.util;

import org.apache.commons.collections.map.LRUMap;

import java.util.Map;

/**
 * Created by root on 2017/6/19.
 */
public class IPCacheParser extends IPParser {

    public IPCacheParser() {
        super();
    }

    //单例模式
    public static IPCacheParser getSingleIPParser() {
        return IP2AreaParserSingle.INSTANCE;
    }

    private static class IP2AreaParserSingle {
        private static IPCacheParser INSTANCE = new IPCacheParser();
    }
    // cache
    private static Map<String, String> cacheIPkv = null;

    // 尝试从catch中获取ip对应area
    @Override
    public String getArea(String logIP) {
        if (null == cacheIPkv) {
            cacheIPkv = new LRUMap(10000);
        }
        String area = cacheIPkv.get(logIP);
        if (null == area) {
            area = super.getArea(logIP);
        }
        return area;
    }


}
