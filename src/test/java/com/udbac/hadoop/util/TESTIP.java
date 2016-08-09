package com.udbac.hadoop.util;

import com.udbac.hadoop.util.IPSeeker;

public class TESTIP {
	public static void main(String[] args) {
        IPSeeker ipSeeker = IPSeeker.getInstance();
        System.out.println(ipSeeker.getCountry("120.197.87.216"));
    }
}
