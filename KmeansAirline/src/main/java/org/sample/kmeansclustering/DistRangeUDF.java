/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.kmeansclustering;

import org.apache.spark.sql.api.java.UDF1;

/**
 *
 * @author Rama
 */
public class DistRangeUDF implements UDF1<String,String>{
    @Override
    
    public String call(String dist) {
        if (Integer.parseInt(dist) <= 250)
            return "very short";
        else if(Integer.parseInt(dist) > 250 && Integer.parseInt(dist)<=500)
            return "short";
        else if (Integer.parseInt(dist) > 500 && Integer.parseInt(dist)<= 1000)
            return "medium";
        else
            return "long";
    }
    
}
