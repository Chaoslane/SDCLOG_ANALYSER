package com.udbac.hadoop.etl.util;

import com.udbac.hadoop.util.UserAgentUtil;
import org.junit.Test;

/**
 * Created by root on 2016/7/22.
 */
public class UserAgentUtilTest {
    @Test
    public void analyticUserAgent() throws Exception {
        System.out.println(new UserAgentUtil().analyticUserAgent("Mozilla/5.0+(iPhone;+CPU+iPhone+OS+9_3_2+like+Mac+OS+X)+AppleWebKit/601.1.46+(KHTML,+like+Gecko)+Mobile/13F69+(5505985024)"));
    }

}