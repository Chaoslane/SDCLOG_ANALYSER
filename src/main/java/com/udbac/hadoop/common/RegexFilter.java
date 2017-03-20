package com.udbac.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/3/17.
 */
public class RegexFilter extends Configured implements PathFilter {
    Pattern pattern;
    FileSystem fs;

    @Override
    public boolean accept(Path path) {
        try {
            if (fs.isDirectory(path)) {
                return true;
            } else {
                pattern = Pattern.compile(getConf().get("filename.pattern"));
                Matcher m = pattern.matcher(path.toString());
                System.out.println("Is path : " + path.toString() + " matches " + getConf().get("filename.pattern") + " ? , " + m.matches());
                return m.matches();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}