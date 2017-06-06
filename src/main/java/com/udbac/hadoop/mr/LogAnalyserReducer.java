package com.udbac.hadoop.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * Created by root on 2017/6/6.
 */
public class LogAnalyserReducer extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }
}
