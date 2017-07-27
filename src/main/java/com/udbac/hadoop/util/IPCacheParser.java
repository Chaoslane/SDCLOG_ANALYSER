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
            cacheIPkv = new LRUMap(1024*100);
        }
        String area = cacheIPkv.get(logIP);
        if (null == area) {
            area = super.getArea(logIP);
            cacheIPkv.put(logIP, area);
        }
        return area;
    }
}
