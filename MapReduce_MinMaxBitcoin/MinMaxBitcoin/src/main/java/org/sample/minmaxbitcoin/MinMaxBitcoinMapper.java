/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.minmaxbitcoin;
import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This is the mapper class which converts the timestamp into month and store min and max value for each
 * @author Rama
 */
public class MinMaxBitcoinMapper  extends  Mapper<LongWritable, Text, Text, MinMax> {
    private Text month = new Text();
    private MinMax minmaxValue = new MinMax();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
       if (key.get() > 0) {
            String[] lines = new CSVParser().parseLine(value.toString());
            month.set(epochConverter(Long.parseLong(lines[0])));
            minmaxValue.setMinValue(Double.parseDouble(lines[7]));
            minmaxValue.setMaxValue(Double.parseDouble(lines[7]));
            context.write(month,minmaxValue);
        }
    
    }
    // this converts the time stamp in to month/year format
    public String epochConverter(Long timeStamp) throws InterruptedException {
        Date date = new Date(timeStamp*1000);
        DateFormat format = new SimpleDateFormat("yyyy/MM");
        String formatted = format.format(date);
        return formatted;
    }
}

    

/*public class FlightsByCarrierMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] lines = new
                    CSVParser().parseLine(value.toString());
            context.write(new Text(lines[8]), new IntWritable(1)); //the 8th index is that for the name of airline carrier
        }
    }
}*/
