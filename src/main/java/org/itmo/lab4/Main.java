package org.itmo.lab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "lab4-bromles");

        job.setJarByClass(Main.class);
        job.setPartitionerClass(CustomNaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(CustomNaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CustomCompositeKeyComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
