package com.mycompany.spktest;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import smile.data.DataFrame;
import smile.data.vector.IntVector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class YoutubeTitleWordCount {
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args) throws IOException {
       // Logger.getLogger("org").setLevel(Level.ERROR);
        // CREATE SPARK CONTEXT
//        SparkConf conf = new SparkConf ().setAppName ("wordCounts").setMaster ("local[3]");
//        JavaSparkContext sparkContext = new JavaSparkContext(conf);
//        // LOAD DATASETS
//        JavaRDD<String> videos = sparkContext.textFile("src/main/resources/data/USvideos.csv");
        
        KmeanClustering clustering =
        		new KmeanClustering();
        clustering.Clusters("src/main/resources/data/USvideos.csv", new int[] {7,8});
//        // TRANSFORMATIONS
//        JavaRDD<String> titles = videos
//                .map(YoutubeTitleWordCount::extractChannelTitle)
//                .filter(StringUtils::isNotBlank);
//       // JavaRDD<String>
//        JavaRDD<String> words = titles.flatMap(title -> Arrays.asList(title
//                .toLowerCase()
//                .trim()
//                .replaceAll("\\p{Punct}", " ")
//                 ).iterator());
//        System.out.println(words.toString());
//         //COUNTING
//      Map<String, Long> wordCounts = words.countByValue ();
//      List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
//              .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
//      // DISPLAY
//      for (Map.Entry<String, Long> entry : sorted) {
//          System.out.println (entry.getKey () + " : " + entry.getValue ());
//      }
//        
//        YoutubeDoa youtubeDoa = new YoutubeDoa();
//        DataFrame trainData =youtubeDoa.readCSV ("src/main/resources/data/titanic-passengers.csv");
//        trainData = trainData.merge (IntVector.of ("Gender", youtubeDoa.encodeCategory(trainData, "Sex")));
//        System.out.println ("=======Encoding Non Numeric Data==============");
//        System.out.println (trainData.structure ());
//        System.out.println (trainData.summary ());
        
        
        
 
    }
    public static String extractChannelTitle(String videoLine) {
        try {
            return videoLine.split (COMMA_DELIMITER)[3];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }
}
