package org.itmo.lab4.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortingReducer extends Reducer<DoubleWritable, CompositeData, Text, Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<CompositeData> values, Context context) throws IOException, InterruptedException {
        for (CompositeData value : values) {
            Text category = new Text(value.getCategory());

            context.write(category, new Text(String.format("%.2f\t%d", key.get(), value.getQuantity())));
        }
    }
}
