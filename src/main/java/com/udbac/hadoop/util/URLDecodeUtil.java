package com.udbac.hadoop.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/7/28.
 */
public class URLDecodeUtil{

   private static final Pattern patternRESC = Pattern.compile("(%5[Cc]|\\\\[xX])([a-fA-F0-9]{2})");
   private static final Pattern patternRE25 = Pattern.compile("%(25)+");
   private static final Pattern patternREPC = Pattern.compile("(%[a-fA-F0-9]{2})");


    public static String decodeSafe(String url) {
        // 转换C风格\x为%
        Matcher matcherRESC = patternRESC.matcher(url);
        if (matcherRESC.find()) {
            url = url.replace("\\x", "%");
        }
        // 转换多个%2525为%
        Matcher matcherRE25 = patternRE25.matcher(url);
        if (matcherRE25.find()) {
            url = matcherRE25.replaceAll("%");
        }
        Matcher matcherREPC = patternREPC.matcher(url);
        if (matcherREPC.find()) {
            try {
                url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
                url = url.replaceAll("\\+", "%2B");
                url = URLDecoder.decode(url, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return url;
    }



    public static void main(String[] args) {
        String a = "ABC\\xE6\\x90\\x9C\\xE7\\x8B\\x90\\xE5\\xB9\\xBF\\xE5\\x91\\x8A/SDK/V6.0.2";
        Matcher matchera = patternRESC.matcher(a);
        if (matchera.find()) {
            System.out.println(a.replace("\\x","%"));
        }

        String b = "asfdasf%25e4%25b8%252525252525ad%25e5%259b%25bdasdfad";
        Matcher matcherb = patternRE25.matcher(b);
        if (matcherb.find()) {
            System.out.println(matcherb.replaceAll("%"));
        }

        String c = "aaa%25e4";
        Matcher matcherc = patternREPC.matcher(c);
        System.out.println(matcherc.find());

        String d = "%zzaaaaa%e4%b8%ad%e5%9b%bdaaaaa";
        System.out.println(decodeSafe(d));
    }
}
