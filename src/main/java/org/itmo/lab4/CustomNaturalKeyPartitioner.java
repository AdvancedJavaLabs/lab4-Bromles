package org.itmo.lab4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomNaturalKeyPartitioner extends Partitioner<Text, DoubleWritable> {
    @Override
    public int getPartition(Text key, DoubleWritable val, int numPartitions) {
        int hash = key.hashCode();
        return hash % numPartitions;
    }
}
