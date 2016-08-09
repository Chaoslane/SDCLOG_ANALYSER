package com.udbac.hadoop.util;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 时间控制工具类
 */
public class TimeUtil {
    public static final String DATE_FORMAT = "yyyyMMdd";

    /**
     * 获取昨日的日期格式字符串数据
     *
     * @return
     */
    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }

    /**
     * 获取对应格式的时间字符串
     *
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     *
     * @param input
     * @return
     */
    public static boolean isValidateRunningDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{4}[0-9]{2}[0-9]{2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }

    /**
     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
     *
     * @param input
     * @return
     */
    public static long parseStringDate2Long(String input) {
        return parseString2Long(input, DATE_FORMAT);
    }

    /**
     * 将指定格式的时间字符串转换为时间戳
     *
     * @param input
     * @param pattern
     * @return
     */
    public static long parseString2Long(String input, String pattern) {
        Date date = null;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }

    /**
     * 将时间戳转换为yyyy-MM-dd格式的时间字符串
     *
     * @param input
     * @return
     */
    public static String parseLong2StringDate(long input) {
        return parseLong2String(input, DATE_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的字符串
     *
     * @param input
     * @param pattern
     * @return
     */
    public static String parseLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 将nginx服务器时间转换为时间戳，如果说解析失败，返回-1
     *
     * @param input 1459581125.573
     * @return
     */
    public static long parseNginxServerTime2Long(String input) {
        Date date = parseNginxServerTime2Date(input);
        return date == null ? -1L : date.getTime();
    }

    /**
     * 将nginx服务器时间转换为date对象。如果解析失败，返回null
     *
     * @param input 格式: 1449410796.976
     * @return
     */
    public static Date parseNginxServerTime2Date(String input) {
        if (StringUtils.isNotBlank(input)) {
            try {
                long timestamp = Double.valueOf(Double.valueOf(input.trim()) * 1000).longValue();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timestamp);
                return calendar.getTime();
            } catch (Exception e) {
                // nothing
            }
        }
        return null;
    }

    /**
     * 获取time指定周的第一天的时间戳值
     *
     * @param time
     * @return
     */
    public static long getFirstDayOfThisWeek(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }


    public static final String TIME_FORMAT = "HHmmss";
    /**
     * long转化为String类型的time formatType:HHmmss
     */

    public static String longToTime(long currentTime){
        return longToTime(currentTime, TIME_FORMAT);
    }
    public static String longToTime(long currentTime, String formatType) {
        Date dateOld = new Date(currentTime); // 根据long类型的毫秒数生命一个date类型的时间
        String sDateTime = dateToString(dateOld, formatType); // 把date类型的时间转换为string
        Date date = null; // 把String类型转换为Date类型
        try {
            date = stringToDate(sDateTime, formatType);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateToString(date,TIME_FORMAT);
    }

    public static long timeToLong(String strTime){
        return timeToLong(strTime, TIME_FORMAT);
    }

    /**
     * String类型的时间 转化为long类型的毫秒数 formatType:HH:mm:ss
     */
    public static long timeToLong(String strTime, String formatType) {
        if (strTime.length() != 6) {
            return 3;
        }
        Date date = null; // String类型转成date类型
        try {
            date = stringToDate(strTime, formatType);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (date == null) {
            return 0;
        } else {
            long currentTime = dateToLong(date); // date类型转成long类型
            return currentTime;
        }
    }
    public static String dateToString(Date date, String formatType) {
        return new SimpleDateFormat(formatType).format(date);
    }
    public static long dateToLong(Date date) {
        return date.getTime();
    }
    public static Date stringToDate(String strTime, String formatType)
            throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd"+formatType);
        Date date = null;
        date = formatter.parse("19700102"+strTime);
        return date;
    }
}
