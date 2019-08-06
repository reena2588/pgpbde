package com.upgrad.pgpbde.project.map.reduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/** 
 * The <b> SaavnStreamingMapper </b> class is an implementation of the <b> Mapper </b> class which is used to filter the Saavn streaming data before it is sent to the partitioner class <b> SaavnStreamingPartitioner </b>.
 * <p> 
 * All the map output to be partitioned on a particular <b> trendingDay </b> is sent the partitioner class.
 */

public class SaavnStreamingMapper extends Mapper<Object, Text, SaavnStreamingKey, SaavnStreamingValue> {
	
	/** A constant which defines the first trending day i.e. it represents 25th December. */
	final int FIRST_TRENDING_DAY = 25;
	
	/** A constant which defines the last trending day i.e. it represents 31st December. */
	final int LAST_TRENDING_DAY  = 31;
	
	/** 
	 * This method writes map output based on the streamingDay and trending window defined. 
	 * */	
	public void writeMap(String songId, int streamingDay, int songWeight, int trendDuration, Context context) throws InterruptedException, IOException{
		
		Text day =  new Text(Integer.toString(streamingDay)); 
		Text id  =  new Text(songId);
		
		/** The startTrendingDay is defined. */
		int startTrendingDay = streamingDay+1;
		
		/** The endTrendingDay is defined. */
		int endTrendingDay   = streamingDay+trendDuration;
		
		if (startTrendingDay<FIRST_TRENDING_DAY){
			
			startTrendingDay = FIRST_TRENDING_DAY;
			
		}else if (endTrendingDay>LAST_TRENDING_DAY){
			
			endTrendingDay = LAST_TRENDING_DAY;
		}
		
		if (startTrendingDay>=FIRST_TRENDING_DAY && endTrendingDay<=LAST_TRENDING_DAY){
		
			for (int trendingDay=startTrendingDay; trendingDay<=endTrendingDay; trendingDay++){
				
				context.write(new SaavnStreamingKey(day, id), new SaavnStreamingValue(trendingDay, songWeight));
				
			}
			
		}
		
	}
	
	
	/**
	 * An implementation of the <b> map() </b> method in the Mapper class <b> SaavnStreamingMapper </b> which filters the Saavn song streaming records and generates the map output in the following format:
	 * <p>
	 * <b> { SaavnStreamingKey(day, id), SaavnStreamingValue(trendingDay, songWeight)} </b>
	 * <p>
	 * For example:
	 * <b> {(25,vHtuvXz), (25, 1350)} </b>
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		/** A constant which defines the total number of fields in a streaming record. */
		final short TOTAL_FIELDS_PER_RECORD = 5;
		
		/** A constant which defines the position of the songId field in the streaming record. */
		final short POSITION_OF_SONG_ID     = 0;
		
		/** A constant which defines the position of the hour field in the streaming record. */
		final short POSITION_OF_HOUR        = 3;
		
		/** A constant which defines the position of the date field in the streaming record. */
		final short POSITION_OF_DATE        = 4;
		
		/** A constant which defines the length of the date field in the streaming record. */
		final short DATE_LENGTH             = 3;
		
		/** A constant which defines the position of day in the date field of the streaming record. */
		final short POSITION_OF_DAY         = 2;
		
		final String TRENDING_WINDOW        = "TRENDING_WINDOW";
		
		/** A Saavn song streaming record is split to extract fields and store them in a String array. */
		String[] streamRecord = value.toString().split(",");
		 
		/** The streaming record is processed only if the total number of fields retrieved from the record is equal to the value defined in the constant TOTAL_FIELDS_PER_RECORD. */
		if (streamRecord.length == TOTAL_FIELDS_PER_RECORD){
			
			/** Retrieves the value of songId from the streaming record and stores it in a variable for further processing. */
			String songId          = streamRecord[POSITION_OF_SONG_ID];
			
			/** Retrieves the value of streamingHour from the streaming record and stores it in a variable for further processing. */
			String streamingHour   = streamRecord[POSITION_OF_HOUR];
			
			/** Retrieves the value of streamingDate from the streaming record and stores it in an array variable for further processing. */
			String[] streamingDate = streamRecord[POSITION_OF_DATE].split("-");
			
			/** The streaming record is further processed only if the following conditions are true. */
			if(streamingDate.length == DATE_LENGTH && songId!=null && !songId.isEmpty()){
		
				int streamingDay = Integer.parseInt(streamingDate[POSITION_OF_DAY]);
				
				/** Calculate the songWeight and store it in a variable. */
				int songWeight = Integer.parseInt(streamingHour) * streamingDay * 1;
				
				/** Retrieve the trending window duration (in days) which was stored in the object of class Configuration while initiating the MapReduce job. */
				Configuration conf = context.getConfiguration();
				int trendDuration  = Integer.parseInt(conf.get(TRENDING_WINDOW));
				
				/** Calculate the trendDurationLowerLimit to make sure the mapper process only a bunch of selected streaming records based on the trending window defined. */
				int trendDurationLowerLimit  = FIRST_TRENDING_DAY-trendDuration; 
				
				/** Call the writeMap() method based on the trending window defined. */
				if(streamingDay >=trendDurationLowerLimit && streamingDay <=LAST_TRENDING_DAY){
					
					writeMap(songId, streamingDay, songWeight, trendDuration, context);
			    				    
				}
			
			}
			
		}
    
	}

}