/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.minmaxbitcoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * This is the main class which start the program execution
 * @author Rama
 */
public class MinMaxBitcoin {
        public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(MinMaxBitcoin.class);
        job.setJobName("MinMaxBitcoin");

        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MinMaxBitcoinMapper.class);
        job.setReducerClass(MinMaxBitcoinReducer.class);

        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMax.class);
        //output format for value in the custom MinMax class type

        job.waitForCompletion(true);
    }
    
}
