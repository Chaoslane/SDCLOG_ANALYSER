package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.CombinationKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2016/8/1.
 */
public class LogAnalyserReducer extends Reducer<CombinationKey, Text, CombinationKey, Text> {

    @Override
    protected void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, new Text(value.toString()));
        }
    }
}
