package com.udbac.hadoop.util;

import org.apache.commons.collections.map.LRUMap;

import java.util.Collections;
import java.util.Map;

/**
 * Created by root on 2017/6/19.
 */
public class IPCacheParser extends IPParser {

    // Suppresses default constructor, ensuring non-instantiability.
    private IPCacheParser() {
        super();
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

    //单例模式
    public static IPCacheParser getSingleIPParser() {
        return IP2AreaParserSingle.INSTANCE;
    }

    private static class IP2AreaParserSingle {
        private static IPCacheParser INSTANCE = new IPCacheParser();
    }
}
