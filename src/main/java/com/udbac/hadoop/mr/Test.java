package com.udbac.hadoop.mr;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2017/6/9.
 */
public class Test {
    public static void main(String[] args) {
        List<Long> list = new ArrayList<>();
        list.add(1l);
        list.add(2l);
        list.add(2l);
        System.out.println(list.get(list.size()-1)-list.get(list.size()-2));
    }
}
