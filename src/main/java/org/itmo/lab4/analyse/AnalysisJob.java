package org.itmo.lab4.analyse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class AnalysisJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = Integer.parseInt(args[2]);

        Configuration configuration = getConf();

        Job analysisJob = Job.getInstance(configuration, "analysis");

        analysisJob.setNumReduceTasks(reducerCount); // reducer scaling

        analysisJob.setJarByClass(AnalysisJob.class);

        analysisJob.setMapperClass(AnalysisMapper.class);
        analysisJob.setReducerClass(AnalysisReducer.class);

        analysisJob.setMapOutputKeyClass(Text.class);
        analysisJob.setMapOutputValueClass(SalesData.class);

        analysisJob.setOutputKeyClass(Text.class);
        analysisJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(analysisJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(analysisJob, new Path(outputDir));

        boolean success = analysisJob.waitForCompletion(true);

        if (!success) {
            return 1;
        }

        return 0;
    }
}
