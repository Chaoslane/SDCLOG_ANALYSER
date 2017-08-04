package com.udbac.hadoop.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.regex.Matcher;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by root on 2017/7/31.
 */
@DisplayName("URLDecodeUtil")
class URLDecodeUtilTest {


    @Test
    void decodeSafe() {
        URLDecodeUtil.decodeSafe("");
    }

}