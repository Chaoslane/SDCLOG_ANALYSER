package com.udbac.hadoop.common;

import com.udbac.hadoop.util.URLDecodeUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.Parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/8/4.
 */
public class UserAgent {
    private static Parser parser = new CachingParser();

    private String catalog = "Other";
    private String browser = "Other";
    private String browserver = "";
    private String os = "Other";
    private String osver = "";
    private String device = "Other";
    private String brand = "";
    private String model = "";

    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        map.put("catalog", this.catalog);
        map.put("browser", this.browser);
        map.put("browserver", this.browserver);
        map.put("os", this.os);
        map.put("osver", this.osver);
        map.put("device", this.device);
        map.put("brand", this.brand);
        map.put("model", this.model);
        return map;
    }

    private UserAgent() {}

    public UserAgent(String uaStr) {
        if (StringUtils.isNotBlank(uaStr)) {

            String[] vs = uaStr.split("[^A-Za-z0-9_-]", -1);
            //优酷
            switch (vs[0]) {
                case "Youku":
                case "Tudou": {
                    String[] vs1 = uaStr.split("[;]", -1);
                    if (vs1.length == 5) {
                        this.catalog = "Youku";
                        this.browser = vs1[0];
                        this.browserver = vs1[1];
                        this.os = vs1[2];
                        this.osver = vs1[3];
                        this.device = replaceIllegal(URLDecodeUtil.decodeSafe(vs1[4]));
                        this.brand = "";
                        this.model = "";
                        //model 1...50
                        if (StringUtils.isNotBlank(vs1[4]) && vs1[4].length() < 50) {
                            this.model =replaceIllegal(URLDecodeUtil.decodeSafe(vs1[4]));
                        }
                    } else if (vs1.length == 4) {
                        String[] vs2 = uaStr.split("[ ;/]", -1);
                        if (vs2.length == 6) {
                            this.catalog = "Youku";
                            this.browser = vs2[0];
                            this.browserver = vs2[2] + "/" + vs2[3] + " " + vs2[1];
                            this.os = vs2[4];
                            this.osver = vs2[5];
                            this.device = "";
                            this.brand = "";
                            this.model = "";
                        }
                    } else {
                        // youku default
                        this.catalog = "Youku_Other";
                    }
                    break;
                }
                case "QYPlayer":
                case "Cupid": {
                    String[] vs1 = uaStr.split("[;/]", -1);
                    if (vs1.length == 2) {
                        this.catalog = "iQiyi";
                        this.browser = vs1[0];
                        this.browserver = vs1[1];
                        this.os = "";
                        this.osver = "";
                        this.device = "";
                        this.brand = "";
                        this.model = "";
                    } else if (vs1.length == 3) {
                        this.catalog = "iQiyi";
                        this.browser = vs1[0];
                        this.browserver = vs1[2];
                        this.os = vs1[1];
                        this.osver = "";
                        this.device = "";
                        this.brand = "";
                        this.model = "";
                    } else {
                        // iqiyi default
                        this.catalog = "iQiyi_Other";
                    }
                    break;
                }
                default:
                    handleClient(parser.parse(uaStr));
                    break;
            }
        }
    }

    private void handleClient(Client client) {
        if (null != client) {
            this.catalog = "Normal";
            this.browser = client.userAgent.family;
            this.browserver = buildVersion(
                    client.userAgent.major, client.userAgent.minor, client.userAgent.patch);
            this.os = client.os.family;
            this.osver = buildVersion(
                    client.os.major, client.os.minor, client.os.patch);
            this.device = replaceIllegal(URLDecodeUtil.decodeSafe(client.device.family));
            this.brand = replaceIllegal(URLDecodeUtil.decodeSafe(client.device.brand));
            this.model = replaceIllegal(URLDecodeUtil.decodeSafe(client.device.model));
        }
    }

    private static String buildVersion(String major, String minor, String patch) {
        if (StringUtils.isBlank(major)) {
            return "";
        }
        if (StringUtils.isBlank(minor)) {
            return major;
        } else if (StringUtils.isBlank(patch)) {
            return major + "." + minor;
        } else {
            return major + "." + minor + "." + patch;
        }
    }


    private static String replaceIllegal(String string) {
        return string.replaceAll("[^A-Za-z0-9 /_-]", "");
    }

}
