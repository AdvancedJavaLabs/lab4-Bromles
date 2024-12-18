package org.itmo.lab4.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class SortingJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = Integer.parseInt(args[2]);

        Configuration configuration = getConf();

        Job sortingJob = Job.getInstance(configuration, "sorting");

        sortingJob.setNumReduceTasks(reducerCount); // reducer scaling

        sortingJob.setJarByClass(SortingJob.class);

        sortingJob.setSortComparatorClass(SortingComparator.class);

        sortingJob.setMapperClass(SortingMapper.class);
        sortingJob.setReducerClass(SortingReducer.class);

        sortingJob.setMapOutputKeyClass(DoubleWritable.class);
        sortingJob.setMapOutputValueClass(CompositeData.class);

        sortingJob.setOutputKeyClass(CompositeData.class);
        sortingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortingJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(sortingJob, new Path(outputDir));

        boolean success = sortingJob.waitForCompletion(true);

        if (!success) {
            return 1;
        }

        return 0;
    }
}
