package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 2016/7/25.
 */
public class CombinationKey implements WritableComparable<CombinationKey> {
    private static final Logger logger = Logger.getLogger(CombinationKey.class);
    private String firstKey;
    private int secondKey;

    @Override
    public int compareTo(CombinationKey o) {
        return this.firstKey.compareTo(o.getFirstKey());
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        firstKey = in.readUTF();
        secondKey = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!out.equals(null)) {
            out.writeUTF(firstKey);
            out.writeInt(secondKey);
        }
    }

    public CombinationKey() {
        super();
    }

    public CombinationKey(String firstKey, int secondKey) {
        super();
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }


    public String getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public int getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(int secondKey) {
        this.secondKey = secondKey;
    }
}
