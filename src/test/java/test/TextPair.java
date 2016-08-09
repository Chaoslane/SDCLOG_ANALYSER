package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable {

    private Text first;
    private Text second;
    private Text third;

    public TextPair(Text first, Text second, Text third) {
        set(first, second, third);
    }

    public TextPair() {
        set(new Text(), new Text(), new Text());
    }

    public TextPair(String first, String second, String third) {
        set(new Text(first), new Text(second), new Text(third));
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public Text getThird() {
        return third;
    }

    public void set(Text first, Text second, Text third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        third.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        third.write(out);
    }

    @Override
    public String toString() {
        return first + " " + second + " " + third;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode() + third.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second)
                    && third.equals(tp.third);
        }
        return false;
    }

    @Override
    public int compareTo(Object tp) {

        if (tp instanceof TextPair) {

            int cmp = first.compareTo(((TextPair) tp).first);

            if (cmp != 0) {
                return cmp;
            }

            int cmp1 = second.compareTo(((TextPair) tp).second);

            if (cmp1 != 0) {
                return cmp1;
            }
            return third.compareTo(((TextPair) tp).third);

        }
        return -1;

    }

}