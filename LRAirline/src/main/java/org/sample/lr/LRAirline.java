/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.sample.lr;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.month;
import org.apache.spark.sql.types.DataTypes;
/**
 *
 * @author Rama
 */
public class LRAirline {
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

VectorAssembler assembler = new VectorAssembler().setInputCols(features).setOutputCol("rawFeatures");

Dataset<Row>[] splits = indexedDF.select(col("Satisfaction_idx"),col("Status_idx"),col("PctFgt_idx"),col("code_idx"),
                    col("FgtMIN_idx"),col("sens_idx"),col("PctFgt_idx"),col("FgtDist_idx"),col("sens_idx"),col("deptHR_idx"),col("arrival_idx")).randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];
    System.out.println("We have training examples count :: " + splits[0].count() + " and test examples count ::" + splits[1].count());
    //trainingData.show();
    
// Trains a k-means model.
    LogisticRegression dt = new LogisticRegression()
                                        .setLabelCol("Satisfaction_idx")
                                        .setFeaturesCol("rawFeatures")
                                        .setMaxIter(10)
                                        .setRegParam(0.3)
                                        .setElasticNetParam(0.8);
    
    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler,dt});
    PipelineModel pipelineModel = pipeline.fit(trainingData);
    //KMeansModel model = kmeans.fit(indexedDF);

    // Make predictions
    Dataset<Row> predictions = pipelineModel.transform(testData);

    predictions.show();

    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Satisfaction_idx")
      .setPredictionCol("prediction")
      .setMetricName("accuracy");
    double accuracy = evaluator.evaluate(predictions);
    System.out.println("Test Error = " + (1.0 - accuracy));

    // $example off$

    spark.stop();
  }
   
    
}
