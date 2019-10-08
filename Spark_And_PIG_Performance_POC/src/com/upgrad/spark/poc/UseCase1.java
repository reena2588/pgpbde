package com.upgrad.spark.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UseCase1 {

	public static void main(String[] args) {
		
		JavaSparkContext jsc  = null;
		
		String inputFile      = args[0].trim();
		String outputFile     = args[1].trim();
		
		final String APP_NAME = "Spark_Ang_PIG_Performance_POC_UseCase_1";
		
		try {
			
			SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

			jsc = new JavaSparkContext(sparkConf);
			
	        JavaRDD<String> rows = jsc.textFile(inputFile);
	        
	        JavaRDD<String> dataWithoutHeader = rows.filter(
	        		row -> {
	        			
	        				String[] columns = row.split(",");
	        		
	        				if (columns[0].equals("VendorID")){
	        		
	        					return false;
	        		
	        				} 
	        				
	        				return true;
	        				
	        			}
	        		
	        );     
	        
	        JavaRDD<String> filteredData = dataWithoutHeader.filter(
	        		row -> {
	        				String[] columns = row.split(",");
	        				
	        				if (columns[0].equals("2") && columns[1].equals("2017-10-01 00:15:30") && columns[2].equals("2017-10-01 00:25:11") && columns[3].equals("1") && columns[4].equals("2.17")){
	        					
	        					return true;
	        				
	        				} 
	        					
	        				return false;
	        			
	        			}
	        );
	        	       
	        filteredData.saveAsTextFile(outputFile);
        	        	        
		}catch (Exception e){
			
			e.printStackTrace();
			
		}finally{
		
			if (jsc!=null){
			
				jsc.close();
				
			}
		
		}
		
	}
	
}