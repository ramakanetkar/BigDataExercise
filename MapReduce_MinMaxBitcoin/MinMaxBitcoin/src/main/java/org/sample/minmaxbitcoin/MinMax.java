/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.minmaxbitcoin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
/**
 * This class is made so that we can capture the min and max value as a part of single object
 * @author Rama
 */
public class MinMax  implements Writable{
    private Double minValue;
    private Double maxValue;

    public Double getMinValue() {
        return minValue;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public void setMinValue(Double minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Double maxValue) {
        this.maxValue = maxValue;
    }

    public MinMax() {
        this.minValue = 0.0;
        this.maxValue = 0.0;
    }
    
    public void readFields(DataInput in) throws IOException{
        minValue = in.readDouble();
        maxValue = in.readDouble();
    }
    
    public void write(DataOutput out)  throws IOException {
        out.writeDouble(minValue);
        out.writeDouble(maxValue);
    }

    
    public String toString() {
        return "MIN: " + minValue + "  Max:" + maxValue;
    }
    
    
}
