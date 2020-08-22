/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.minmaxbitcoin;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * This is the reducer class
 * @author Rama
 */
public class MinMaxBitcoinReducer extends
        Reducer<Text, MinMax, Text, MinMax> {
    private MinMax output = new MinMax();

    @Override
    protected void reduce(Text token, Iterable<MinMax> counts,
                          Context context) throws IOException, InterruptedException {
    output.setMinValue(null);
    output.setMaxValue(null);
    for (MinMax count : counts) {
        if(output.getMinValue() == null || count.getMinValue() < output.getMinValue())
            output.setMinValue(count.getMinValue());
        if(output.getMaxValue() == null || count.getMaxValue() > output.getMaxValue())
            output.setMaxValue(count.getMaxValue());

    }
    context.write(token,output);

}
}
/*public class FlightsByCarrierReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text token, Iterable<IntWritable> counts,
                          Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable count : counts) {
            sum+= count.get();
        }
        context.write(token, new IntWritable(sum));*/