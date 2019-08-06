package com.upgrad.pgpbde.project.map.reduce;

import java.util.HashMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
 * The <b> SaavnStreamingPartitioner </b> class is used to partition the map output on <b> trendingDay </b> before sending the partitioned output to the reducer class <b> SaavnStreamingReducer </b>.
 * <p>
 * All the map output partitioned on a particular trendingDay is sent to a specific reducer for aggregation.
 */

public class SaavnStreamingPartitioner extends Partitioner<SaavnStreamingKey, SaavnStreamingValue> implements Configurable {

	private Configuration configuration;
	
	/** The HashMap<String, Integer> stores the <b> trendingDay </b> as Key and <b> Reducer Number </b> as Value. 
	 *  Since we have 07 trendingDays i.e 25th December to 31st December (both inclusive), we have 07 reducers numbered 0 to 6.  
	 * */
	private HashMap<String, Integer> dayMap = new HashMap<>();

	/** Each <b> trendingDay </b> is mapped to a unique reducer number for the map output to be partitioned accordingly. */
	@Override
	public void setConf(Configuration configuration) {
		
		this.configuration = configuration;
		
		dayMap.put("25", 0);
		dayMap.put("26", 1);
		dayMap.put("27", 2);
		dayMap.put("28", 3);
		dayMap.put("29", 4);
		dayMap.put("30", 5);
		dayMap.put("31", 6);
	
	}


	@Override
	public Configuration getConf() {
		
		return configuration;
	
	}

	/** This method returns the partition number based on the value of the <b> trendingDay </b> from the <b> HashMap<String, Integer> dayMap </b> */
	public int getPartition(SaavnStreamingKey key, SaavnStreamingValue value, int numReduceTasks) {
		
		return (int) dayMap.get(String.valueOf(value.getTrendingDay()));
		
	}

}
