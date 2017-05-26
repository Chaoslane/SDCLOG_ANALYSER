package com.udbac.hadoop.common;

/**
 * Created by root on 2017/1/16.
 */
public class LogConstants {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    public static final String TIME_FORMAT = "HH:mm:ss";
    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unknown";
    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 半小时的毫秒数
     */
    public static final int HALFHOUR_OF_MILLISECONDS = 1800000;

    public static String INPUTARGSWARN =
            "Usage args:" + "\n" +
                    "-Dfilename.pattern=.*StringRegex.* " + "\n" +
                    "-Dfields.column=field1,field2,field3 " + "\n" +
                    "<inputPath> " + "\n" +
                    "<outputPath> ";

}
