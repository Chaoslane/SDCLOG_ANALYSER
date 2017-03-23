package com.udbac.hadoop.util;

/**
 * Created by root on 2017/1/16.
 */
import com.udbac.hadoop.common.LogConstants;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by root on 2017/1/12.
 * IP解析为地域名称
 */
public class IPv42AreaUtil {
    private static final String udbacIPtransSegs = "/udbacIPtransSegs.csv";
    private static final String udbacIPtransArea = "/udbacIPtransArea.csv";
    private static List<Integer> sortedList;
    private static Map<Integer, String> mapSegs ;
    private static Map<String, String> mapArea ;

    private IPv42AreaUtil() {
    }

    static{
        try {
            initialize(IPv42AreaUtil.class.getResourceAsStream(udbacIPtransSegs)
                    ,IPv42AreaUtil.class.getResourceAsStream(udbacIPtransArea));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void initialize(InputStream udbacSegsInputStream , InputStream udbacAreaInputStream) throws IOException {
        mapSegs = new HashMap<>();
        sortedList = new ArrayList<>();
        List<String> readSeges = IOUtils.readLines(udbacSegsInputStream);
        for (String oneline : readSeges) {
            String[] strings = oneline.split(LogConstants.SEPARTIOR_TAB);
            Integer startIPInt = IPv4Util.ipToInt(strings[0]);
            mapSegs.put(startIPInt,strings[2]);
            sortedList.add(startIPInt);
        }
        sortedList.sort(Comparator.naturalOrder());

        mapArea = new HashMap<>();
        List<String> readAreas = IOUtils.readLines(udbacAreaInputStream);
        for (String oneline : readAreas) {
            String[] strings = oneline.split(LogConstants.SEPARTIOR_TAB);
            mapArea.put(strings[2], strings[0]+LogConstants.SEPARTIOR_COMMA+strings[1]);
        }
    }

    /**
     * 解析为 province,city
     * @param logIP IP字符串
     * @return  province,city
     * @throws IOException
     */
    public static String getArea(String logIP){
        return mapArea.get(getIPcode(logIP));
    }

    /**
     * 获取文件IPcode
     * @param logIP IP字符串
     * @return IPcode
     * @throws IOException
     */
    public static String getIPcode(String logIP){
        Integer index = searchIP(sortedList, IPv4Util.ipToInt(logIP));
        return mapSegs.get(sortedList.get(index));
    }
    /**
     * 二分查找 ip 在有序 list 中的 index
     * @param sortedList ip转化成整数后 sort
     * @param ipInt ipToInt
     * @return index
     */
    private static Integer searchIP(List<Integer> sortedList, Integer ipInt) {
        int mid = sortedList.size() / 2;
        if (sortedList.get(mid) == ipInt) {
            return mid;
        }
        int start = 0;
        int end = sortedList.size() - 1;
        while (start <= end) {
            mid = (end - start) / 2 + start;
            if (ipInt < sortedList.get(mid)) {
                end = mid - 1;
                if(ipInt > sortedList.get(mid-1)){
                    return mid - 1;
                }
            } else if (ipInt > sortedList.get(mid)) {
                start = mid + 1;
                if (ipInt < sortedList.get(mid + 1)) {
                    return mid;
                }
            } else {
                return mid;
            }
        }
        return 0;
    }
}
