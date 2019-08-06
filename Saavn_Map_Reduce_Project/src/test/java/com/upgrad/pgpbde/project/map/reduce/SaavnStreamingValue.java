package com.upgrad.pgpbde.project.map.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** 
 * The <b> SaavnStreamingValue </b> class is used as the Output Value class for the Mapper class <b> SaavnStreamingMapper </b>. 
 * <p>
 * A Map Output Value is constructed using an object of this class.
 * <p>
 * It contains <b> trendingDay </b> and <b> songWeight </b> as its members and overrides the following methods:
 * <p>
 * readFields()
 * <p>
 * write()
 */

public class SaavnStreamingValue implements Writable {
	
	/** The value for the <b> trendingDay </b> member could be any value between 25th December to 31st December (both days inclusive). 
	 *  It is the day on which a particular song is trending.
	 * */
	private int trendingDay;
	
	/** The value for the <b> songWeight </b> member is the weight of a particular instance of a song streamed. 
	 * It is calculated using the following formula:
	 * <p>
	 * songWeight = streamingHour * streamingDay * 1
	 *  */
	private int songWeight;

	public SaavnStreamingValue() {
		
		super();
		
	}

	public SaavnStreamingValue(int trendingDay, int songWeight) {
	    
		this.trendingDay = trendingDay;
	    this.songWeight   = songWeight;
	
	}
	
	public int getTrendingDay() {
		return trendingDay;
	}

	public void setTrendingDay(int trendingDay) {
		this.trendingDay = trendingDay;
	}
	
	public int getSongWeight() {
		return songWeight;
	}

	public void setSongWeight(int songWeight) {
		this.songWeight = songWeight;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		trendingDay = WritableUtils.readVInt(dataInput);
	    songWeight  = WritableUtils.readVInt(dataInput);      
	 
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
	    
		WritableUtils.writeVInt(dataOutput, trendingDay);
	    WritableUtils.writeVInt(dataOutput, songWeight);

	}

}