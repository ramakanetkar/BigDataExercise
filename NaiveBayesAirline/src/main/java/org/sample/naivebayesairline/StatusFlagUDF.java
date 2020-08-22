/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.naivebayesairline;

import org.apache.spark.sql.api.java.UDF1;

/**
 *
 * @author Rama
 */
public class StatusFlagUDF implements UDF1<String,Integer>{
    @Override
    
    public Integer call(String a) {
        
        if (a.equalsIgnoreCase("Blue"))
            return 4;
        else if (a.equalsIgnoreCase("Silver"))
            return 3;
        else if (a.equalsIgnoreCase("Gold"))
            return 2;
        else 
            return 1;
    }
    
}
