package org.itmo.lab4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomNaturalKeyGroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return w1.compareTo(w2);
    }
}
