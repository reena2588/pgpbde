package com.upgrad.spark.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UseCase3 {

	public static void main(String[] args) {
		
		JavaSparkContext jsc  = null;
		
		String inputFile      = args[0].trim();
		String outputFile     = args[1].trim();
		
		final String APP_NAME = "Spark_Ang_PIG_Performance_POC_UseCase_3";
		
		try {
			
			SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

			jsc = new JavaSparkContext(sparkConf);
			
	        JavaRDD<String> rows = jsc.textFile(inputFile);
	        
	        JavaRDD<String> dataWithoutHeader = rows.filter(
	        		row -> {
	        			
	        				String[] columns = row.split(",");
	        		
	        				if (columns[0].equals("VendorID") || columns[0].equals("") || columns[0]==null){
	        		
	        					return false;
	        		
	        				} 
	        				
	        				return true;
	        				
	        		}
	        		
	        );     
	        
	        JavaPairRDD<String, Long> paymentType = dataWithoutHeader.mapToPair(
	        		row -> {
	        		
	        				String[] vals = row.split(",");
	        		
	        				return new Tuple2<String, Long>(vals[9], 1L);
	        		
	        		}
	        		
	        );
	        
	        JavaPairRDD<String, Long> finalStat = paymentType.reduceByKey((x,y) -> x+y);
	        
	        JavaPairRDD<Long, String> xchangeKey = finalStat.mapToPair(
	        		tuple -> {
	        					Long key = tuple._2;
	        					
	        					String value = tuple._1;
	        					
	        					return new Tuple2<Long, String>(key,value);
	        		}
	        );
	        
	        JavaPairRDD<Long, String> orderData = xchangeKey.sortByKey(false);
	        
	        JavaPairRDD<String, Long> exchangeKeyAgain = orderData.mapToPair(
	        		tuple -> {
	        					Long value = tuple._1;
	        					
	        					String key = tuple._2;
	        					
	        					return new Tuple2<String, Long>(key,value);
	        		}
	        );
	        
	        exchangeKeyAgain.saveAsTextFile(outputFile);
        	        	        
		}catch (Exception e){
			
			e.printStackTrace();
			
		}finally{
		
			if (jsc!=null){
			
				jsc.close();
				
			}
		
		}
		
	}
	
}