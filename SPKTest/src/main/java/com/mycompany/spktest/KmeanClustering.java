package com.mycompany.spktest;
import java.awt.Color;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import smile.data.DataFrame;
import smile.io.Read;
import smile.plot.swing.Canvas;
import smile.plot.swing.Histogram;
import smile.plot.swing.ScatterPlot;

public class KmeanClustering {
static int numofzeros = 0; 
public void Clusters(String path,int columns[]) {
	
	  SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
      JavaSparkContext sparkContext = new JavaSparkContext(conf);
	
      JavaRDD<String> data = sparkContext.textFile(path);//read().csv(path);
	  JavaRDD<Vector> parsedData =	data.zipWithIndex().map(s-> {
		//System.out.println(s._2()+"  "+"first"+s._1());
		if(s._2()>0 ) {
		      double [] view_like = new double[columns.length];	
		      //System.out.println("the string"+s._1());
		      for(int i=0; i<columns.length;i++) {
		    	  view_like[i] = parseColumn(s._1(),columns[i]);
		    	 // System.out.println("the value"+view_like[i]);
		      }
		      
			  return Vectors.dense(view_like);
		}
		else
		{
			 return Vectors.dense(new double[] {0,0});
		}
		
	});
	parsedData.cache();

	// Cluster the data into two classes using KMeans
	int numClusters = 4;
	int numIterations = 20;
	KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
	System.out.println("Cluster centers:");
	for (Vector center: clusters.clusterCenters()) {
	  System.out.println(" " + center);
	}
	double cost = clusters.computeCost(parsedData.rdd());
	System.out.println("Cost: " + cost);

	// Evaluate clustering by computing Within Set Sum of Squared Errors
	double WSSSE = clusters.computeCost(parsedData.rdd());
	System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	//JavaDoubleRDD  test = parsedData.collect(Collectors.toList())
	// Save and load model
	
   
}
public DataFrame readCSV(String path) {
    CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader ().withDelimiter(';');
    DataFrame df = null;
    try {
        df = Read.csv (path, format);
    } catch (IOException e) {
        e.printStackTrace ();
    } catch (URISyntaxException e) {
        e.printStackTrace ();
    }
    System.out.println (df.summary ());
    return df;
}
public static double parseColumn(String videoLine,int index) {
	
	
    try {
        return Double.parseDouble(videoLine.split(";")[index]);
    } catch (Exception e) {
    	numofzeros++;
    	System.out.println("nums of zeros"+numofzeros);
        return 0;
    }
    
}
}
