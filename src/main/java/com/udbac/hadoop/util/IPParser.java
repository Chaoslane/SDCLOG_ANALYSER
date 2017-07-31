package com.udbac.hadoop.util;

/**
 * Created by root on 2017/1/16.
 */

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by root on 2017/1/12.
 * IP解析为地域名称
 */
public class IPParser {
    private static final String udbacIPtransSegs = "/udbacIPtransSegs.csv";
    private static final String udbacIPtransArea = "/udbacIPtransArea.csv";
    private static List<Integer> sortedList;
    private static Map<Integer, String> mapSegs;
    private static Map<String, String> mapArea;

    public IPParser(){
        initialize(IPParser.class.getResourceAsStream(udbacIPtransSegs)
                , IPParser.class.getResourceAsStream(udbacIPtransArea));
    }

    private void initialize(InputStream udbacSegsInputStream, InputStream udbacAreaInputStream) {
        mapSegs = new HashMap<>();
        sortedList = new ArrayList<>();
        try {
            List<String> readSeges = IOUtils.readLines(udbacSegsInputStream);
            for (String oneline : readSeges) {
                String[] strings = oneline.split("\t");
                Integer startIPInt = ipToInt(strings[0]);
                mapSegs.put(startIPInt, strings[2]);
                sortedList.add(startIPInt);
            }
            Collections.sort(sortedList);
            mapArea = new HashMap<>();
            List<String> readAreas = IOUtils.readLines(udbacAreaInputStream);
            for (String oneline : readAreas) {
                String[] strings = oneline.split("\t");
                mapArea.put(strings[2], strings[0] + "," + strings[1]);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 解析为 province,city
     *
     * @param logIP IP字符串
     * @return [province city]
     */
    public String[] getArea(String logIP) {
        return mapArea.get(getIPcode(logIP)).split(",");
    }

    /**
     * 获取文件IPcode
     *
     * @param logIP IP字符串
     * @return IPcode
     * @throws IOException
     */
    private String getIPcode(String logIP) {
        Integer index = searchIP(sortedList, ipToInt(logIP));
        return mapSegs.get(sortedList.get(index));
    }

    /**
     * 二分查找 ip 在有序 list 中的 index
     *
     * @param sortedList ip转化成整数后 sort
     * @param ipInt      ipToInt
     * @return index
     */
    private Integer searchIP(List<Integer> sortedList, Integer ipInt) {
        int low = 0;
        int high = sortedList.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            if (ipInt < sortedList.get(mid)) {
                if (ipInt > sortedList.get(mid - 1)) {
                    return mid - 1;
                }
                high = mid - 1;
            } else if (ipInt > sortedList.get(mid)) {
                if (ipInt < sortedList.get(mid + 1)) {
                    return mid;
                }
                low = mid + 1;
            } else {
                return mid;
            }
        }
        return 0;
    }

    /**
     * 把IP地址转化为int
     * @param ipAddr
     * @return int
     */
    private static int ipToInt(String ipAddr) {
        try {
            return bytesToInt(ipToBytesByInet(ipAddr));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return 1000000000;
        }
    }

    /**
     * 根据位运算把 byte[] -> int
     * @param bytes
     * @return int
     */
    private static int bytesToInt(byte[] bytes) {
        int addr = bytes[3] & 0xFF;
        addr |= ((bytes[2] << 8) & 0xFF00);
        addr |= ((bytes[1] << 16) & 0xFF0000);
        addr |= ((bytes[0] << 24) & 0xFF000000);
        return addr;
    }

    /**
     * 把IP地址转化为字节数组
     * @param ipAddr
     * @return byte[]
     */
    private static byte[] ipToBytesByInet(String ipAddr) throws UnknownHostException {
        return InetAddress.getByName(ipAddr).getAddress();
    }


    public static class Cache extends IPParser {
        // cache
        private static Map<String, String[]> cacheIPkv = null;

        // 尝试从catch中获取ip对应area
        @Override
        public String[] getArea(String logIP) {
            if (null == cacheIPkv) {
                cacheIPkv = new LRUMap(1024*100);
            }
            String[] area = cacheIPkv.get(logIP);
            if (null == area) {
                area = super.getArea(logIP);
                cacheIPkv.put(logIP, area);
            }
            return area;
        }
    }

//    public static void main(String[] args) {
//        IPParser ipParser = new IPParser();
//        System.out.println(ipParser.getArea("0.0.0.0"));
//    }
}
