package com.udbac.hadoop.common;

/**
 * Created by root on 2016/7/12.
 */
public class SDCLogConstants {
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
    /**
     * 定义的运行时间变量名
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";
    /**
     * 日志分隔符
     */
    public static final String LOG_SEPARTIOR = " ";
    /**
     * 动作发生的日期
     */
    public static final String LOG_COLUMN_NAME_Date = "date";
    /**
     * 动作发生的时间（UTC标准时间）
     */
    public static final String LOG_COLUMN_NAME_Time = "time";
    /**
     * App客户端IP地址
     */
    public static final String LOG_COLUMN_NAME_Cip = "c-ip";
    /**
     * App服务器地址，原生App数据采集数据中此项为空。
     */
    public static final String LOG_COLUMN_NAME_Sip = "s-ip";
    /**
     *请求中使用的http方法，get/post
     */
    public static final String LOG_COLUMN_NAME_CsMethod = "cs-method";
    /**
     * 访问文件URI主体，原生App采集动作假名，HTML页面采集URI
     */
    public static final String LOG_COLUMN_NAME_CsUriStem = "cs-uri-stem";
    /**
     * 记录URL查询参数，此字段中存放SDK采集的自定义业务与用户行为数据
     */
    public static final String LOG_COLUMN_NAME_CsUriQuery = "cs-uri-query";
    /**
     * 记录http状态代码，200表示成功
     */
    public static final String LOG_COLUMN_NAME_ScStatus = "sc-status";
    /**
     * HTTP协议版本，如http/1.0
     */
    public static final String LOG_COLUMN_NAME_CsVersion = "cs-version";
    /**
     * 客户端浏览器、操作系统等情况，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsUserAgent = "cs(User-Agent)";
    /**
     * 客户端cookies内容，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsCookie = "cs(Cookie)";
    /**
     * 用户此动作时的前向链接，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsReferer = "cs(Referer)";

    //QUERY里面的数据
    /**
     * 渠道 例如IOS WAP SUPERAPP
     */
    public static final String LOG_QUERY_NAME_UTMSOURCE="utm_source";
    /**
     * 用户唯一ID
     */
    public static final String LOG_QUERY_NAME_DEVICEID="wt.co_f";
    /**
     * 用户类型
     */
    public static final String LOG_QUERY_NAME_WTUTYPE="WT.branch";
    /**
     * APP下载渠道
     */
    public static final String LOG_QUERY_NAME_WTAVV="wt.avv";
    /**
     * APP点击位置
     */
    public static final String LOG_QUERY_NAME_WTPOS="wt.sr";
    /**
     * wt.event
     */
    public static final String LOG_QUERY_NAME_WTEVENT="wt.event";
    /**
     * wt.event
     */
    public static final String LOG_QUERY_NAME_WTMSG="wt.msg";

    /**
     * 登录标识
     */
    public static final String LOG_EVENT_NAME_WTLOGIN="wt.login";
    /**
     * 浏览菜单标识
     */
    public static final String LOG_EVENT_NAME_WTMENU="wt.menu";
    /**
     * 用户信息页标识
     */
    public static final String LOG_EVENT_NAME_WTUSER="wt.user";
    /**
     * 购物车确认页标识
     */
    public static final String LOG_EVENT_NAME_WTCART="wt.cart";
    /**
     * 订单成功标识
     */
    public static final String LOG_EVENT_NAME_WTSUC="wt.suc";
    /**
     * 订单支付标识
     */
    public static final String LOG_EVENT_NAME_WTPAY="wt.pay";

    /**
     * 操作系统名称
     */
    public static final String LOG_COLUMN_NAME_OS_NAME = "os";
    /**
     * 操作系统版本
     */
    public static final String LOG_COLUMN_NAME_OS_VERSION = "os_v";
    /**
     * 浏览器名称
     */
    public static final String LOG_COLUMN_NAME_BROWSER_NAME = "browser";
    /**
     * 浏览器版本
     */
    public static final String LOG_COLUMN_NAME_BROWSER_VERSION = "browser_v";

    }
