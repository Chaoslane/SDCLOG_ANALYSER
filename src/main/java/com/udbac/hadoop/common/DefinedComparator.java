package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by root on 2016/7/25.
 */
public class DefinedComparator extends WritableComparator {
    private static final Logger logger = Logger.getLogger(CombinationKey.class);

    public DefinedComparator() {
        super(CombinationKey.class, true);
    }

    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable CombinationKeyOther) {
        logger.debug("---------enter DefinedComparator flag---------");

        CombinationKey c1 = (CombinationKey) combinationKeyOne;
        CombinationKey c2 = (CombinationKey) CombinationKeyOther;

        if (!c1.getFirstKey().equals(c2.getFirstKey())) {
            logger.debug("---------out DefinedComparator flag---------");
            return c1.getFirstKey().compareTo(c2.getFirstKey());
        } else {
            //按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2)
            logger.debug("---------out DefinedComparator flag---------");
            //0,负数,正数
            return Integer.compare(c1.getSecondKey(), c2.getSecondKey());
        }

    }
}
