package org.itmo.lab4.analyse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalysisMapper extends Mapper<Object, Text, Text, SalesData> {
    private final Text categoryKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length == 5 && !fields[0].equals("transaction_id")) {
            String category = fields[2];

            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);

            categoryKey.set(category);

            context.write(categoryKey, new SalesData(price * quantity, quantity));
        }
    }
}
