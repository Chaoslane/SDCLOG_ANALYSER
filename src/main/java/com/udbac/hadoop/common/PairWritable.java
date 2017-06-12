package com.udbac.hadoop.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 2017/6/6.
 */
public class PairWritable implements Writable, WritableComparable<PairWritable> {

    private String cookieId;
    private String dateTime;

    public PairWritable() {
    }

    public PairWritable(String cookieId, String dateTime) {
        this.cookieId = cookieId;
        this.dateTime = dateTime;
    }

    public String getCookieId() {
        return cookieId;
    }

    public String getDateTime() {
        return dateTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, cookieId);
        WritableUtils.writeString(out, dateTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cookieId = WritableUtils.readString(in);
        dateTime = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(PairWritable o) {
        int res = this.cookieId.compareTo(o.cookieId);
        if (res == 0) {
            res = this.dateTime.compareTo(o.dateTime);
        }
        return res;
    }


    public static class PairPartitioner extends Partitioner<PairWritable, Text> {

        @Override
        public int getPartition(PairWritable pairWritable, Text text, int numPartitions) {
            return Math.abs(pairWritable.cookieId.hashCode()) % numPartitions;
        }
    }

    public static class PairComparator extends WritableComparator {

        public PairComparator() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable pair1 = (PairWritable) a;
            PairWritable pair2 = (PairWritable) b;
            int res = pair1.cookieId.compareTo(pair2.cookieId);
            if (res == 0) {
                res = pair1.dateTime.compareTo(pair2.dateTime);
            }
            return res;
        }
    }

    public static class PairGrouping extends WritableComparator {

        public PairGrouping() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(Object a, Object b) {
            PairWritable pair1 = (PairWritable) a;
            PairWritable pair2 = (PairWritable) b;
            return pair1.cookieId.compareTo(pair2.cookieId);
        }
    }

}
