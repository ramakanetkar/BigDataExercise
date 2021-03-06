/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.kmeansalcohol;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;


/**
 *
 * @author Rama
 */
public class KMAlcohol {
    public static void main(String[] args) {
        
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("JavaNaiveBayesExample").getOrCreate();
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
	
        Dataset<Row> ds = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/rkanetka/input/DigitalBreathTestData2014.csv");

        ds.cache();
        ds.select("Reason").show();;
        ds.show();
        System.out.println("Our dataset has rows :: " + ds.count());

        Dataset<Row> df = ds.drop("Year");
        df.printSchema();
        
        Dataset<Row> processedDF = df.select(col("Reason"),
                                            col("Month"),
                                            col("WeekType"),
                                            col("TimeBand"),
                                            col("BreathAlcoholLevel(microg 100ml)").cast(DataTypes.IntegerType),
                                            col("AgeBand"),
                                            col("Gender")
                                            );
        //processedDF.printSchema();
        UDF1 modifyWeekday = new DayConverterUDF();
        sparkSession.udf().register("modify", modifyWeekday, DataTypes.IntegerType);
        processedDF = processedDF.withColumn("WeekdayFlag", callUDF("modify",col("WeekType")));
        
        UDF1 alcoholLevel = new AlcoholLevelUDF();
        sparkSession.udf().register("level",alcoholLevel, DataTypes.StringType);
        processedDF = processedDF.withColumn("AlcoholLevel", callUDF("level",col("BreathAlcoholLevel(microg 100ml)")));
               
        StringIndexer reasonInd = new StringIndexer().setInputCol("Reason").setOutputCol("reasonInd");
        StringIndexer monthInd = new StringIndexer().setInputCol("Month").setOutputCol("monthInd");
        StringIndexer weekInd = new StringIndexer().setInputCol("WeekdayFlag").setOutputCol("weekTypeInd");
        StringIndexer timeBandInd = new StringIndexer().setInputCol("TimeBand").setOutputCol("timeBandInd");
        StringIndexer ageBandInd = new StringIndexer().setInputCol("AgeBand").setOutputCol("ageBandInd");
        StringIndexer genderInd = new StringIndexer().setInputCol("Gender").setOutputCol("genderInd");
        StringIndexer levelInd = new StringIndexer().setInputCol("AlcoholLevel").setOutputCol("levelInd");
        
        df.printSchema();
   
        Dataset<Row> indexedDF = reasonInd.fit(processedDF).transform(processedDF);
        indexedDF=monthInd.fit(indexedDF).transform(indexedDF);
        indexedDF=weekInd.fit(indexedDF).transform(indexedDF);
        indexedDF=timeBandInd.fit(indexedDF).transform(indexedDF);
        indexedDF=ageBandInd.fit(indexedDF).transform(indexedDF);
        indexedDF=genderInd.fit(indexedDF).transform(indexedDF);
        indexedDF=levelInd.fit(indexedDF).transform(indexedDF);
            
      
       String[] featuresCols = {"monthInd","weekTypeInd","timeBandInd","reasonInd","genderInd","ageBandInd","BreathAlcoholLevel(microg 100ml)"};
        
        for (String str : featuresCols) {
            System.out.println(str + " :: ");
        }
        
        //indexedDF.show();      
           
        VectorAssembler vectorAssembler = new VectorAssembler()
                                          .setInputCols(featuresCols)
                                          .setOutputCol("features");
        
        Dataset<Row> data = vectorAssembler.transform(indexedDF);
        data.show();
        Dataset<Row>[] split = data.select(col("reasonInd"),col("monthInd"),col("weekTypeInd"),col("timeBandInd"),col("BreathAlcoholLevel(microg 100ml)"),
                                            col("ageBandInd"),col("genderInd"),col("levelInd"),col("features")).randomSplit(new double[]{0.7, 0.3});
        System.out.println("We have training examples count :: " + split[0].count() + " and test examples count ::" + split[1].count());

        Dataset<Row> trainingData = split[0];
        
        Dataset<Row> testData = split[1];
        
       KMeans km = new KMeans().setFeaturesCol("features").setK(10);
       KMeansModel model = km.fit(trainingData);
       Dataset<Row> predictions = model.transform(testData);
        
       predictions.show();
              
       // Evaluate clustering by computing Silhouette score
ClusteringEvaluator evaluator = new ClusteringEvaluator();

double silhouette = evaluator.evaluate(predictions);
System.out.println("Silhouette with squared euclidean distance = " + silhouette);
       Vector[] centers = model.clusterCenters();
       System.out.println("Cluster Centers: ");
             for (Vector center: centers) {
                 System.out.println(center);
             }

       
       /*
       Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{vectorAssembler,km});
       
       PipelineModel pipelineModel=pipeline.fit(trainingData);
    
       Dataset<Row> predictions = pipelineModel.transform(testData);
        
       predictions.show();
       
               // compute accuracy on the test set
       MulticlassClassificationEvaluator evaluator 
            = new MulticlassClassificationEvaluator().setLabelCol("reasonInd").setPredictionCol("prediction").setMetricName("accuracy");
    
       double accuracy = evaluator.evaluate(predictions);
       System.out.println("Test set accuracy = " + accuracy);
*/

       sparkSession.stop();
        

    }
}
