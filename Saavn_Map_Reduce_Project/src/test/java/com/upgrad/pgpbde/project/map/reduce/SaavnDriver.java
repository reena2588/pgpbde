package com.upgrad.pgpbde.project.map.reduce;

import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;

/** 
 * The <b> SaavnDriver </b> class is the main class of the program which initiates the map reduce job. 
 * <p> 
 * It provides a solution for finding the top 100 trending songs for each day between 25th December to 31st December from the Saavn data store.
 * <p>
 * It takes input path, output path and trending window as input via the command-line arguments and produces 07 output files one for each day between 25th December to 31st December, each file listing the top 100 trending songs for the day. 
 */
 
public class SaavnDriver extends Configured implements Tool{


	public int run(String[] args) throws IOException, InterruptedException{
		
			/** A constant value indicating the number of reducers to be invoked. We have chosen 07 as there are 07 days between 25th December to 31st December. */
			final int NO_OF_REDUCERS     = 7;
			
			/** A constant indicating the name of the Map Reduce Job */
			final String JOB_NAME        = "EXTRACT_100_TRENDING_SONGS_FOR_SAAVN";
			
			/** The first command-line argument is the absolute path to the input file location i.e. the location where the Saavn data exists. */ 
			final String INPUT_PATH      = args[0];
			
			/** The second command-line argument is the absolute path to the output file location i.e. the location where the output of the program is expected to be stored. */
			final String OUTPUT_PATH     = args[1];
			 
			/** The third command-line argument is the duration (in days) of the trending window. The trending window is the duration (in days) which is considered for finding the trending songs for a particular day. */
			final String TRENDING_WINDOW = args[2];
		
			Configuration conf = new Configuration();
			
			/** 
			 * The duration (in days) for the trending window is passed to the Mapper class <b> SaavnStreamingMapper </b> via the object of the Configuration class.
			 * The trending window duration is going to be used for identifying the top 100 trending songs for a particular day.
			 */
			conf.set("TRENDING_WINDOW", TRENDING_WINDOW);
						
	    	Job streamData = Job.getInstance(conf, JOB_NAME);
	    	
	    	streamData.setJarByClass(SaavnDriver.class);

	    	streamData.setMapperClass(SaavnStreamingMapper.class);
	    	streamData.setPartitionerClass(SaavnStreamingPartitioner.class);
	    	streamData.setReducerClass(SaavnStreamingReducer.class);
	    	
	    	streamData.setNumReduceTasks(NO_OF_REDUCERS);
	    	
	    	streamData.setMapOutputKeyClass(SaavnStreamingKey.class);
	    	streamData.setMapOutputValueClass(SaavnStreamingValue.class);
	    	
	    	streamData.setOutputKeyClass(Text.class);
	    	streamData.setOutputValueClass(NullWritable.class);
	    	 
	    	FileInputFormat.addInputPath(streamData,   new Path(INPUT_PATH));
	    	FileOutputFormat.setOutputPath(streamData, new Path(OUTPUT_PATH));

			    	           	
	    	try {
			
	    		if (!streamData.waitForCompletion(true)) {
      		  
	    			System.exit(1);
    		
	    		}
		
	    	} catch (Exception e) {
			
	    		e.printStackTrace();
		
	    	} 
    	
    	return 0;
    	     
    }
 

	public static void main(String[] args) throws Exception {
									
			ToolRunner.run(new Configuration(), new SaavnDriver(), args);
		
	}

}