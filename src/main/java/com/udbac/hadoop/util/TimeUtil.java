package com.udbac.hadoop.util;

import com.udbac.hadoop.common.Constants;
import com.udbac.hadoop.common.LogParseException;

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


    private static SimpleDateFormat dateTimeFormat = new SimpleDateFormat(Constants.DATETIME_FORMAT);

    /**
     * 采集机时区问题 时间+8 hour 保证日志时间范围是1天内
     *
     * @param dateTime datetime
     * @return datetime
     */
    public static String handleTime(String dateTime) throws LogParseException {
        try {
            Date date = new Date(dateTimeFormat.parse(dateTime).getTime() + 8 * 3600 * 1000);
            return dateTimeFormat.format(date);
        } catch (ParseException e) {
            throw new LogParseException("Unsupported datetime format:" + dateTime, e);
        }
    }

    /**
     * 将指定格式的时间字符串转换为时间戳
     */
    public static long parseString2Long(String input) throws LogParseException {
        try {
            return dateTimeFormat.parse(input).getTime();
        } catch (ParseException e) {
            throw new LogParseException("Unsupported datetime format:" + input);
        }
    }

    /**
     * 将时间戳转换为指定格式的字符串
     */
    public static String parseLong2String(long input) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return dateTimeFormat.format(calendar.getTime());
    }

    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     */
    public static boolean isValidateDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{2}-[0-9]{2}-[0-9]{2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }
}
