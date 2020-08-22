/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.gaussianmixtureairline;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
/**
 *
 * @author Rama
 */
public class GMAirline {
    public static void main(String[] args) {
    // Create a SparkSession.
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaKMeansExample")
      .getOrCreate();
    Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read()
			      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
			      .option("header", "true")
			      .load("hdfs://spark01.cmua.dom:9000/rkanetka/input/IDUGAirlineSatisfactionSurvey.csv");
    
    /*String[] columns = dataset.columns();
    StringIndexer[] indexer = new StringIndexer[columns.length];
    for (String column: columns) {
       int i = 0;
         indexer[i] = new StringIndexer().setInputCol(column).setOutputCol(column + "_idx");
         i++;
    }*/
    Dataset<Row> processedDF = dataset.select(col("Satisfaction"),
                  col("Airline Status"),
                  col("Airline Code"),
                  col("Flight cancelled"),
                  col("Arrival Delay greater 5 Mins"),
                  col("% of Flight with other Airlines"),
                  col("Price Sensitivity"),
                 //col("\"No. of other Loyalty Cards\"").cast(DataTypes.IntegerType),
                  col("Day of Month"),
                  col("Flight date"),
                  col("Scheduled Departure Hour"),
                  col("Departure Delay in Minutes"),
                  col("Arrival Delay in Minutes"),
                  col("Flight time in minutes"),
                  col("Flight Distance"));
    processedDF.na().drop();
    
    
                UDF1 range = new DistRangeUDF();
        spark.udf().register("range", range, DataTypes.StringType);
        processedDF = processedDF.withColumn("DistRange", callUDF("range",col("Flight Distance")));
        
        UDF1 total = new StatusFlagUDF();
        spark.udf().register("StatusFlag",total, DataTypes.IntegerType);
        processedDF = processedDF.withColumn("StatusFlag", callUDF("StatusFlag",col("Airline Status")));
        
        
    StringIndexer Status_Index = new StringIndexer().setInputCol("Airline Status").setOutputCol("Status_idx").setHandleInvalid("skip");
    StringIndexer Satisfaction_Index = new StringIndexer().setInputCol("Satisfaction").setOutputCol("Satisfaction_idx").setHandleInvalid("skip");
    StringIndexer ArvDelay_Index = new StringIndexer().setInputCol("StatusFlag").setOutputCol("ArvDelay_idx").setHandleInvalid("skip");
    StringIndexer PctFgt_Index = new StringIndexer().setInputCol("% of Flight with other Airlines").setOutputCol("PctFgt_idx").setHandleInvalid("skip");
    StringIndexer sens_Index = new StringIndexer().setInputCol("Price Sensitivity").setOutputCol("sens_idx").setHandleInvalid("skip");
    StringIndexer DOM_Index = new StringIndexer().setInputCol("Day of Month").setOutputCol("DOM_idx").setHandleInvalid("skip");
    StringIndexer date_Index = new StringIndexer().setInputCol("Flight date").setOutputCol("date_idx").setHandleInvalid("skip");
    StringIndexer code_Index = new StringIndexer().setInputCol("Airline Code").setOutputCol("code_idx").setHandleInvalid("skip");
    StringIndexer cancel_Index = new StringIndexer().setInputCol("Flight cancelled").setOutputCol("cancel_idx").setHandleInvalid("skip");
    StringIndexer deptHR_Index = new StringIndexer().setInputCol("Scheduled Departure Hour").setOutputCol("deptHR_idx").setHandleInvalid("skip");
    StringIndexer deptMIN_Index = new StringIndexer().setInputCol("DistRange").setOutputCol("deptMIN_idx").setHandleInvalid("skip");
    StringIndexer arrival_Index = new StringIndexer().setInputCol("Arrival Delay greater 5 Mins").setOutputCol("arrival_idx").setHandleInvalid("skip");
    StringIndexer FgtMIN_Index = new StringIndexer().setInputCol("Flight time in minutes").setOutputCol("FgtMIN_idx").setHandleInvalid("skip");
    StringIndexer FgtDist_Index = new StringIndexer().setInputCol("Flight Distance").setOutputCol("FgtDist_idx").setHandleInvalid("skip");
    
    Dataset<Row> indexedDF = Status_Index.fit(processedDF).transform(processedDF);
                 indexedDF = Satisfaction_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = ArvDelay_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = PctFgt_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = sens_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = DOM_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = date_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = code_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = cancel_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = arrival_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = deptHR_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = deptMIN_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = FgtMIN_Index.fit(indexedDF).transform(indexedDF);
                 indexedDF = FgtDist_Index.fit(indexedDF).transform(indexedDF);

String[] features = {"arrival_idx","Status_idx","code_idx",
                    "FgtMIN_idx","sens_idx","FgtDist_idx","deptHR_idx","sens_idx"};

VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("features");

Dataset<Row> data = assembler.transform(indexedDF);

Dataset<Row>[] splits = data.select(col("Satisfaction_idx"),col("Status_idx"),col("PctFgt_idx"),col("code_idx"),
                    col("FgtMIN_idx"),col("sens_idx"),col("PctFgt_idx"),col("FgtDist_idx"),col("sens_idx"),col("deptHR_idx"),col("arrival_idx"),col("features")).randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];
    
      GaussianMixture gm = new GaussianMixture().setFeaturesCol("features").setK(10);
       
       GaussianMixtureModel model = gm.fit(trainingData);
       
       Dataset<Row> predictions = model.transform(testData);
        
       predictions.show();
               // Output the parameters of the mixture model
         for (int i = 0; i < model.getK(); i++) {
         System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
                 i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
         }
    // $example off$
             ClusteringEvaluator evaluator = new ClusteringEvaluator();

double silhouette = evaluator.evaluate(predictions);
System.out.println("Silhouette with squared euclidean distance = " + silhouette);

       spark.stop();
        

    }
}
