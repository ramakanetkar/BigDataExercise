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
public class AlcoholLevelUDF implements UDF1<Integer,String>{
    @Override
    
    public String call(Integer level) {
        
        if(level <= 50)
            return "low";
        else if(level > 50 && level <= 100)
            return "medium";
        else
            return "high";
    }
    
}
