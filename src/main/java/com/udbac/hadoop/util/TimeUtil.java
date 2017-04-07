package com.udbac.hadoop.util;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.LogParseException;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 时间控制工具类
 */
public class TimeUtil {

    /**
     * 采集机时区问题 时间+8 hour 保证日志时间范围是1天内
     * @param dateTime datetime
     * @return datetime
     */
    public static String handleTime(String dateTime) throws LogParseException {
        String realtime = null;
        AtomicReference<Calendar> calendar;
        calendar = new AtomicReference<>(Calendar.getInstance());
        SimpleDateFormat dateFormat = new SimpleDateFormat(LogConstants.DATETIME_FORMAT);
        try {
            Date date = dateFormat.parse(dateTime);
            calendar.get().setTime(date);
            calendar.get().add(Calendar.HOUR_OF_DAY, 8);
            realtime = (new SimpleDateFormat(LogConstants.DATETIME_FORMAT)).format(calendar.get().getTime());
        } catch (ParseException e) {
            throw new LogParseException("Unsupported datetime format:" + dateTime,e);
        }
        return realtime;
    }

//    /**
//     * 判断输入的参数是否是一个有效的时间格式数据
//     */
//    public static boolean isValidateRunningDate(String input) {
//        Matcher matcher = null;
//        boolean result = false;
//        String regex = "[0-9]{4}.[0-9]{2}.[0-9]{2}.";
//        if (input != null && !input.isEmpty()) {
//            Pattern pattern = Pattern.compile(regex);
//            matcher = pattern.matcher(input);
//        }
//        if (matcher != null) {
//            result = matcher.matches();
//        }
//        return result;
//    }
//
//
//    /**
//     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
//     */
//    public static long parseStringDate2Long(String input) throws LogParseException {
//        return parseString2Long(input, LogConstants.DATETIME_FORMAT);
//    }
//
//
//    /**
//     * 将时间戳转换为yyyy-MM-dd格式的时间字符串
//     */
//    public static String parseLong2StringDate(long input) {
//        return parseLong2String(input, LogConstants.DATETIME_FORMAT);
//    }
//
//
//    /**
//     * 将时间戳转换为指定格式的字符串
//     */
//    public static String parseLong2String(long input, String pattern) {
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeInMillis(input);
//        return new SimpleDateFormat(pattern).format(calendar.getTime());
//    }
//
//
//    /**
//     * 将指定格式的时间字符串转换为时间戳
//     */
//    public static long parseString2Long(String input, String pattern) throws LogParseException {
//        Date date;
//        try {
//            date = new SimpleDateFormat(pattern).parse(input);
//        } catch (ParseException e) {
//            throw new LogParseException("Unsupported datetime format:" + input);
//        }
//        return date.getTime();
//    }

}
