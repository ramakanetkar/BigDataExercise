/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.decisiontreealcohol;
import org.apache.spark.sql.api.java.UDF1;
/**
 *
 * @author Rama
 */
public class DayConverterUDF implements UDF1<String,Integer>{
    @Override
    
    public Integer call(String weekDay) {
        
        if (weekDay.equals("Weekday"))
                return 1;
        else
        return 0;
        
    }
    
}
