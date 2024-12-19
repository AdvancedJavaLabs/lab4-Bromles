package org.itmo.lab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.itmo.lab4.analyse.AnalysisJob;
import org.itmo.lab4.sort.SortingJob;

public class SalesAnalyzer {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar /tmp/lab4-parcalc-bromles-0.0.1.jar org.itmo.lab4.SalesAnalyzer <input path> <output path> <REDUCER_COUNT> <DATABLOCK_SIZE_KB>");
            System.exit(-1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        int reducerCount = Integer.parseInt(args[2]);
        int datablockSizeBytes = Integer.parseInt(args[3]) * 1024; // * 1 kb
        String intermediateResultDir = outputDir + "-intermediate";

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(datablockSizeBytes));

        long startTime = System.currentTimeMillis();

        String[] analysisArgs = new String[]{inputDir, intermediateResultDir, String.valueOf(reducerCount)};

        // Data analysis
        int exitCode = ToolRunner.run(configuration, new AnalysisJob(), analysisArgs);

        if (exitCode != 0) {
            System.out.println("Analysis job failed");
            System.exit(1);
        }

        String[] sortingArgs = new String[]{intermediateResultDir, outputDir, String.valueOf(reducerCount)};

        // Data sorting
        exitCode = ToolRunner.run(configuration, new SortingJob(), sortingArgs);

        long endTime = System.currentTimeMillis();
        System.out.println("Jobs completed in " + (endTime - startTime) + " milliseconds.");

        System.exit(exitCode);
    }
}
